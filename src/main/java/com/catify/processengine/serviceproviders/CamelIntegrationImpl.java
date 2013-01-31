/**
 * -------------------------------------------------------
 * Copyright (C) 2013 catify <info@catify.com>
 * -------------------------------------------------------
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.catify.processengine.serviceproviders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.xml.XPathBuilder;
import org.apache.camel.impl.DefaultCamelContext;

import com.catify.processengine.core.integration.IntegrationMessage;
import com.catify.processengine.core.integration.MessageIntegrationSPI;
import com.catify.processengine.core.processdefinition.jaxb.TMetaData;
import com.catify.processengine.core.services.MessageDispatcherService;

/**
 * The Class CamelIntegrationImpl is a reference implementation of the
 * MessageIntegrationSPI for the Apache Camel framework. <br>
 * 
 * @author christopher köster
 * 
 */
public class CamelIntegrationImpl extends MessageIntegrationSPI {

	/** The camel context. */
	private CamelContext camelContext;
	
	/** The camel template. */
	private ProducerTemplate camelTemplate;
	
	/** The message dispatcher service. */
	protected MessageDispatcherService messageDispatcherService;
	
	/** 
	 * Map that holds meta data names and their xpath expressions. 
	 * <li> Key: 'metaDataName' <br>
	 * <li> Value: 'metaDataXpath' 
	 */
	protected Map<String, String> metaDataXpaths;
	
	/** Map that holds meta data names and their values returned by the xpath query. 
	 * <li> Key: 'metaDataName' <br>
	 * <li> Value: 'metaDataValue' 
	 */
	protected Map<String, Object> metaDataValues;

	/**
	 * Instantiates a new camel integration implementation. The prefix used 
	 * for your implementation and in your bpmn process.xml always needs to 
	 * be defined like this: 
	 * <pre><code>this.prefix = "camel";</code></pre>
	 * If you need a Spring ApplicationContext provide one of your own. Note
	 * that the service providers are dynamically loaded at runtime, so
	 * built-time weaving will not work.
	 * @throws Exception 
	 */
	public CamelIntegrationImpl() throws Exception {
		this.prefix = "camel";
		this.messageDispatcherService = new MessageDispatcherService(this);
		
		this.camelContext = new DefaultCamelContext();
		this.camelContext.start();
		this.camelTemplate = this.camelContext.createProducerTemplate();
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#
	 * startThrowingIntegrationImplementation(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void startSend(
			final String uniqueFlowNodeId, String messageIntegrationString) {

		String endpointUri = this.getEndpointUriFromIntegrationString(messageIntegrationString);

		// put identifier and route uri to the map 
		// to be able to send messages to the correct endpoint later on 
		flowNodeIdIntegrationMap.put(uniqueFlowNodeId, endpointUri);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#
	 * startCatchingIntegrationImplementation(java.lang.String,
	 * java.lang.String, java.util.List)
	 */
	@Override
	public void startReceive(
			final String uniqueFlowNodeId, String messageIntegrationString,
			List<TMetaData> tMetaDatas) {

		// get the endpoint uri for the given flow node
		String endpointUri = this.getEndpointUriFromIntegrationString(messageIntegrationString);

		// fill the map that holds meta data names and their xpath expressions
		this.metaDataXpaths = convertTMetaDataListToMap(tMetaDatas);

		// create a route to consume from (defined by the message integration
		// string in the bpmnProcess.xml)
		try {
			this.camelContext.addRoutes(this.createConsumerRoute(endpointUri,
					uniqueFlowNodeId));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// put identifier and route uri to the map 
		// to be able to send messages to the correct endpoint later on 
		flowNodeIdIntegrationMap.put(uniqueFlowNodeId, endpointUri);
	}

	/* (non-Javadoc)
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#startRequestReplyIntegrationImplementation(java.lang.String, java.lang.String)
	 */
	@Override
	public void startRequestReply(
			String uniqueFlowNodeId, String messageIntegrationString) {
		
		String endpointUri = this.getEndpointUriFromIntegrationString(messageIntegrationString);

		// put identifier and route uri to the map 
		// to be able to send messages to the correct endpoint later on 
		flowNodeIdIntegrationMap.put(uniqueFlowNodeId, endpointUri);
	}
	
	/* (non-Javadoc)
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#shutDownIntegrationImplementation(java.lang.String)
	 */
	@Override
	public boolean shutDownIntegrationImplementation(String uniqueFlowNodeId) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#send(com.catify.processengine.core.integration.IntegrationMessage)
	 */
	@Override
	public void send(IntegrationMessage integrationMessage) {

		// get the endpoint uri via the unique flow node id
		String routeUri = flowNodeIdIntegrationMap.get(integrationMessage.getUniqueFlowNodeId());

		// create headers from the fields of the integrationMessage
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("processId", integrationMessage.getProcessId());
		headers.put("uniqueFlowNodeId", integrationMessage.getUniqueFlowNodeId());
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

	/* (non-Javadoc)
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#receive(com.catify.processengine.core.integration.IntegrationMessage, java.util.Map)
	 */
	@Override
	public void receive(IntegrationMessage integrationMessage,
			Map<String, Object> metaData) {
		this.messageDispatcherService.dispatchToEngine(integrationMessage, metaData);
	}

	/* (non-Javadoc)
	 * @see com.catify.processengine.core.integration.MessageIntegrationSPI#requestReply(com.catify.processengine.core.integration.IntegrationMessage)
	 */
	@Override
	public Object requestReply(IntegrationMessage integrationMessage) {

		// get the endpoint uri via the integrationId
		String routeUri = flowNodeIdIntegrationMap.get(integrationMessage.getUniqueFlowNodeId());

		// create headers from the fields of the integrationMessage
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("processId", integrationMessage.getProcessId());
		headers.put("uniqueFlowNodeId", integrationMessage.getUniqueFlowNodeId());
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

								// mapping the message to the correct flow node id is done via 
								// the corresponding header (it could also be done via the  
								// flowNodeIdIntegrationMap or the routeId)
								IntegrationMessage integrationMessage = new IntegrationMessage(
										ex.getIn().getHeader("processId",
												String.class), ex.getIn()
												.getHeader("uniqueFlowNodeId",
														String.class), ex
												.getIn().getHeader(
														"processInstanceId",
														String.class), ex
												.getIn().getBody());
								
								if (integrationMessage.getUniqueFlowNodeId() == null) {
									integrationMessage.setUniqueFlowNodeId(uniqueFlowNodeId);
								}
								
								receive(integrationMessage,
										metaDataValues);

							}
						}).end();
			}
		};
	}
	
	/**
	 * Extracts the endpoint uri from the integration string (eg. seda://in).
	 * 
	 * @param messageIntegrationString
	 *            the message integration string
	 * @return the endpoint uri from that integration string
	 */
	public String getEndpointUriFromIntegrationString(String messageIntegrationString) {
		return messageIntegrationString;
	}
	
	/**
	 * Gets the meta data specified in the integration implementation.
	 *
	 * @return the meta data
	 */
	public Map<String, Object> getMetaDataValues() {
		return metaDataValues;
	}

	/**
	 * Sets the meta data specified in the integration implementation.
	 *
	 * @param metaData the meta data
	 */
	public void setMetaDataValues(Map<String, Object> metaData) {
		this.metaDataValues = metaData;
	}
	
	/**
	 * Gets the flow node map.
	 *
	 * @return the flow node map
	 */
	public Map<String,String> getFlowNodeMap() {
		return flowNodeIdIntegrationMap;
	}

	/**
	 * Gets the camel context.
	 *
	 * @return the camel context
	 */
	public CamelContext getCamelContext() {
		return camelContext;
	}
}
