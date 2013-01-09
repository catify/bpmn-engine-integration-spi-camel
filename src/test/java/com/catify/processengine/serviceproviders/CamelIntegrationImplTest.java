package com.catify.processengine.serviceproviders;

import java.util.ArrayList;
import java.util.Map;

import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;

import com.catify.processengine.core.integration.IntegrationMessage;
import com.catify.processengine.core.processdefinition.jaxb.TMetaData;

public class CamelIntegrationImplTest extends CamelTestSupport {

	@EndpointInject(uri = "mock:out")
    private MockEndpoint out;
	
	private CamelIntegrationImpl spi;
	
	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		spi = new CamelIntegrationImpl();
		out.reset();
	}
	
	
	@Test
	public void testGetMetaDataXpathsMapFromTMetaDataListIsNull() {
		Map<String, String> map = spi.getMetaDataXpathsMapFromTMetaDataList(null);
		assertNotNull(map);
	}
	
	@Test
	public void testGetMetaDataXpathsMapFromTMetaDataListIsEmpty() {
		ArrayList<TMetaData> list = new ArrayList<TMetaData>();
		Map<String, String> map = spi.getMetaDataXpathsMapFromTMetaDataList(list);
		assertNotNull(map);
		assertEquals(0, map.size());
	}
	
	@Test
	public void testGetMetaDataXpathsMapFromTMetaDataListWithValue() {
		ArrayList<TMetaData> list = new ArrayList<TMetaData>();
		TMetaData data1 = new TMetaData();
		data1.setMetaDataKey("foo");
		data1.setMetaDataXpath("//foo");
		list.add(data1);
		Map<String, String> map = spi.getMetaDataXpathsMapFromTMetaDataList(list);
		assertNotNull(map);
		assertEquals(1, map.size());
		assertEquals("foo", map.keySet().iterator().next());
		assertEquals("//foo", map.get("foo"));
	}
	
	@Test
	public void testGetEndpointUriFromIntegrationString(){
		assertEquals("direct://foo", spi.getEndpointUriFromIntegrationString("direct://foo"));
	}
	
	@Test
	public void testStartSend() {
		spi.startThrowingIntegrationImplementation("1", "seda://send");
		assertEquals(1, spi.getFlowNodeMap().size());
		assertTrue(spi.getFlowNodeMap().containsKey("1"));
		assertEquals("seda://send", spi.getFlowNodeMap().get("1"));
	}
	
	@Test
	public void testSend() throws Exception {
		registerRoute();
		spi.startThrowingIntegrationImplementation("1", "direct-vm://send");
		spi.dispatchIntegrationMessageViaSpiImpl("1", new IntegrationMessage("1", "1", "47", "foo"));
		checkMock();
	}
	
	@Test
	public void testStartReceive() throws InterruptedException {
		spi.startCatchingIntegrationImplementation("8709", "seda://receive", new ArrayList<TMetaData>());
		assertNotNull(spi.getCamelContext().getEndpoint("seda://receive"));
	}
	
	@Test
	public void testStartRequestReply() {
		spi.startRequestReplyIntegrationImplementation("8710", "direct://request_reply");
		assertEquals(1, spi.getFlowNodeMap().size());
		assertTrue(spi.getFlowNodeMap().containsKey("8710"));
		assertEquals("direct://request_reply", spi.getFlowNodeMap().get("8710"));
	}
	
	@Test
	public void testRequestReply() throws Exception {
		registerRoute();
		spi.startRequestReplyIntegrationImplementation("8711", "direct-vm://send");
		String result = (String) spi.requestReplyViaSpiImpl("8711", new IntegrationMessage("1", "8711", "1533", "foo"));
		assertEquals("bar", result);
		checkMock();
	}
	
	/**
	 * creates a dummy route in test context
	 * 
	 * @throws Exception
	 */
	private void registerRoute() throws Exception {
		context.addRoutes(new RouteBuilder() {
			
			@Override
			public void configure() throws Exception {
				from("direct-vm://send")
				.to("mock://out")
				.setBody(constant("bar"));				
			}
		});
	}
	
	/**
	 * standard checks on mock endpoint
	 * 
	 * @throws InterruptedException
	 */
	private void checkMock() throws InterruptedException {
		out.setExpectedMessageCount(1);
		out.assertIsSatisfied(5000);
		assertEquals("foo", out.getExchanges().iterator().next().getIn().getBody());
	}
	
}
