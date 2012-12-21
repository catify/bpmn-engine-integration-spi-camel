package com.catify.processengine.serviceproviders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.xml.XPathBuilder;
import org.apache.camel.spring.SpringCamelContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.catify.processengine.core.integration.IntegrationMessage;
import com.catify.processengine.core.integration.MessageIntegrationSPI;
import com.catify.processengine.core.processdefinition.jaxb.TMetaData;
import com.catify.processengine.core.services.MessageDispatcherService;

/**
 * The Class CamelIntegrationImpl is a reference implementation of the
 * MessageIntegrationSPI for the Apache Camel framework. <br>
 * 
 */
public class CamelIntegrationImpl extends MessageIntegrationSPI {

	// configuration items
	private ApplicationContext springApplicationContext;
	private SpringCamelContext camelContext;
	private ProducerTemplate camelTemplate;
	protected MessageDispatcherService messageDispatcherService;

	/**
	 * Instantiates a new camel integration implementation. The first two line
	 * of the constructor are important and always need to be implemented: <li>
	 * this.prefix = "camel"; --> the prefix used for your implementation and in
	 * your bpmn-process.xml. <li><br>
	 * If you need a Spring ApplicationContext provide one of your own. Note
	 * that the SPI implementations are dynamically loaded at runtime, so
	 * built-time weaving will not work. Use a classic approach like in this
	 * example implementation. <br>
	 */
	public CamelIntegrationImpl() {

		this.prefix = "camel";

		this.springApplicationContext = new ClassPathXmlApplicationContext(
				"META-INF/spring/messageIntegrationContext-camel.xml");
		this.camelContext = (SpringCamelContext) this.springApplicationContext
				.getBean("camel");
		this.camelTemplate = this.camelContext.createProducerTemplate();
		this.messageDispatcherService = new MessageDispatcherService(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#
	 * startCatchingIntegrationImplementation(java.lang.String,
	 * java.lang.String, java.util.List)
	 */
	@Override
	public void startCatchingIntegrationImplementation(
			final String uniqueFlowNodeId, String messageIntegrationString,
			List<TMetaData> metaDataList) {

		// get the endpoint uri for the given flow node
		String endpointUri = this
				.getEndpointUriFromIntegrationString(messageIntegrationString);

		// fill the map that holds meta data names and their xpath expressions
		this.metaDataXpaths = getMetaDataXpathsMapFromTMetaDataList(metaDataList);

		// create a route to consume from (defined by the message integration
		// string in the bpmnProcess.xml)
		try {
			this.camelContext.addRoutes(this.createConsumerRoute(endpointUri,
					uniqueFlowNodeId));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Gets the meta data xpaths map from t meta data list.
	 * 
	 * @param metaDataList
	 *            the meta data list
	 * @return the meta data xpaths map from t meta data list
	 */
	private Map<String, String> getMetaDataXpathsMapFromTMetaDataList(
			List<TMetaData> metaDataList) {
		Map<String, String> metaMap = new HashMap<String, String>();

		for (TMetaData tMetaData : metaDataList) {
			metaMap.put(tMetaData.getMetaDataKey(),
					tMetaData.getMetaDataXpath());
		}

		return metaMap;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#
	 * startThrowingIntegrationImplementation(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void startThrowingIntegrationImplementation(
			final String uniqueFlowNodeId, String messageIntegrationString) {

		String endpointUri = this
				.getEndpointUriFromIntegrationString(messageIntegrationString);

		// put identifier and route uri to the map
		flowNodeIdIntegrationImplMap.put(uniqueFlowNodeId, endpointUri);
	}

	@Override
	public boolean shutDownIntegrationImplementation(String uniqueFlowNodeId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void dispatchIntegrationMessageViaSpiImpl(String uniqueFlowNodeId,
			IntegrationMessage integrationMessage) {

		// get the endpoint uri via the integrationId
		String routeUri = flowNodeIdIntegrationImplMap.get(uniqueFlowNodeId);

		// create headers from the fields of the integrationMessage
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("processId", integrationMessage.getProcessId());
		headers.put("nodeId", integrationMessage.getNodeId());
		headers.put("processInstanceId",
				integrationMessage.getProcessInstanceId());

		Object messageBody = null;
		if (integrationMessage.getPayload() == null) {
			messageBody = "no payload";
		} else
			messageBody = integrationMessage.getPayload();

		// send message with camel ProducerTemplate
		camelTemplate.sendBodyAndHeaders(routeUri, messageBody, headers);
	}

	@Override
	public void dispatchToEngine(String uniqueFlowNodeId,
			IntegrationMessage integrationMessage, Map<String, Object> metaData) {

		this.messageDispatcherService.dispatchToEngine(uniqueFlowNodeId,
				integrationMessage, metaData);
	}

	/**
	 * Creates the consumer route for a catching node.
	 * 
	 * @param endpointUri
	 *            the endpoint uri
	 * @param uniqueFlowNodeId
	 *            the unique flow node id
	 * @return the route builder
	 */
	protected RouteBuilder createConsumerRoute(final String endpointUri,
			final String uniqueFlowNodeId) {
		return new RouteBuilder() {

			@Override
			public void configure() throws Exception {

				from(endpointUri).routeId(uniqueFlowNodeId).marshal()
						.string("UTF-8")

						.process(new Processor() {

							@Override
							public void process(Exchange ex) throws Exception {

								metaDataValues = new HashMap<String, Object>();

								// for each key in metaDataXpaths (camel xpath
								// syntax and/or xpath syntax for body only)
								for (String metaDataKey : metaDataXpaths
										.keySet()) {

									String value = XPathBuilder
											.xpath(metaDataXpaths
													.get(metaDataKey))
											.stringResult()
											.evaluate(ex, String.class);

									// add it to the meta data map that hold the
									// actual values
									metaDataValues.put(metaDataKey, value);
								}

								IntegrationMessage integrationMessage = new IntegrationMessage(
										ex.getIn().getHeader("processId",
												String.class), ex.getIn()
												.getHeader("nodeId",
														String.class), ex
												.getIn().getHeader(
														"processInstanceId",
														String.class), ex
												.getIn().getBody());

								dispatchToEngine(uniqueFlowNodeId,
										integrationMessage, metaDataValues);

							}
						}).end();
			}
		};
	}

	/**
	 * Extracts the endpoint uri from the integration string.
	 * 
	 * @param messageIntegrationString
	 *            the message integration string
	 * @return the endpoint uri from that integration string
	 */
	String getEndpointUriFromIntegrationString(String messageIntegrationString) {
		return messageIntegrationString;
	}

	@Override
	public void startRequestReplyIntegrationImplementation(
			String uniqueFlowNodeId, String messageIntegrationString) {
		String endpointUri = this
				.getEndpointUriFromIntegrationString(messageIntegrationString);

		// put identifier and route uri to the map
		flowNodeIdIntegrationImplMap.put(uniqueFlowNodeId, endpointUri);
	}

	@Override
	public Object requestReplyViaSpiImpl(String uniqueFlowNodeId,
			IntegrationMessage integrationMessage) {

		// get the endpoint uri via the integrationId
		String routeUri = flowNodeIdIntegrationImplMap.get(uniqueFlowNodeId);

		// create headers from the fields of the integrationMessage
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("processId", integrationMessage.getProcessId());
		headers.put("nodeId", integrationMessage.getNodeId());
		headers.put("processInstanceId",
				integrationMessage.getProcessInstanceId());

		Object messageBody = null;
		if (integrationMessage.getPayload() == null) {
			messageBody = "no payload";
		} else
			messageBody = integrationMessage.getPayload();

		// send request/reply message with camel ProducerTemplate
		return camelTemplate.requestBodyAndHeaders(routeUri, messageBody,
				headers);
	}

}
