/*
 * LoadRunner Java script. (Build: 3020)
 * 
 * Script Description: 
 *  select customer_id from tborder_action toa, tbban tbb where toa.status = 'DO' and toa.sales_channel = 'CS' and toa.action_type = 'PR' and toa.customer_id = tbb.ban_id and toa.ctdb_cre_datetime > sysdate - 60 and rownum < 20000 order by creation_date DESC                   
 */

import lrapi.lr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

import javax.ejb.CreateException;
import javax.ejb.EJBHome;
import javax.ejb.SessionContext;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.rmi.PortableRemoteObject;
import javax.security.auth.login.LoginContext;

//import amdocs.oms.omscontext.Context;
import amdocs.oms.omsinterface.BanSegmentRequest;
import amdocs.oms.omsinterface.GetBanSegment;
import amdocs.oms.omsinterface.GetBanSegmentRequest;
import amdocs.oms.omsinterface.GetBanSegmentResponse;
import amdocs.oms.sbcapi.NegotiationAPIsInterfaceImpl;
//import amdocs.oms.servicetest.att.OmsClientSimulator;
//import amdocs.oms.servicetest.att.TestService;
//import amdocs.oms.servicetest.att.Utilssss;

//import amdocs.epi.appserver.ClientConnection;
//import amdocs.epi.appserver.ClientConnectionCls;
//import amdocs.oms.servicetest.att.test.Loader;
//import amdocs.oms.servicetest.att.*;
import amdocs.uams.UamsSystem;
import amdocs.uams.login.UamsLoginContext;
import amdocs.uamsx.login.ejb.basic.UamsEjbBasicLogin;
import amdocs.uamsx.login.ejb.basic.UamsEjbBasicLoginHome;

import com.amdocs.cih.common.core.sn.ApplicationContext;
import com.amdocs.cih.common.datatypes.OrderingContext;
import com.amdocs.cih.exception.InvalidUsageException;
import com.amdocs.cih.services.oms.interfaces.IOmsServicesRemote;
import com.amdocs.cih.services.oms.interfaces.IOmsServicesRemoteHome;
import com.amdocs.cih.services.oms.lib.LogoutRequest;
//import com.amdocs.cih.services.oms.rvt.domain.OrderingDomainBasicRVT;
import com.ibm.wsspi.security.token.AttributeNameConstants;
import org.apache.commons.codec.binary.Base64;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

import java.rmi.RemoteException;

import javax.ejb.CreateException;
import javax.naming.InitialContext;
import javax.rmi.PortableRemoteObject;

import com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductConfigurationOptions;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductConfigurationResponse;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.RetrieveProductConfigurationMask;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.RetrieveProductConfigurationRequest;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.RetrieveProductConfigurationResponse;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvc;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.*;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ConfigureAssignedProduct;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductConfigurationOptions;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.UpdateProductConfigurationFilterInfo;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.UpdateProductConfigurationMask;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.UpdateProductConfigurationRequest;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.UpdateProductConfigurationResponse;


import com.amdocs.informationmodel.commonbusinessentities.attachments.AttachmentPurpose;
import com.amdocs.informationmodel.commonbusinessentities.attachments.AttachmentType;
import com.amdocs.informationmodel.customerdomain.customerorder.ProductOrderItemType;
import com.amdocs.services.customerdomain.customerframeworkagreement.AttachmentsFilterInfo;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductConfigurationOptions;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductConfigurationRequest;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.RetrieveProductConfigurationFilterInfo;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.RetrieveProductConfigurationMask;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.RetrieveProductConfigurationRequest;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.RetrieveProductConfigurationResponse;
import com.amdocs.services.productdomain.product.ProductFilterInfo;
import com.amdocs.services.productdomain.product.ProductPresentation;
import com.amdocs.services.productdomain.product.SearchProductsWithHighlightsMask;

//import amdocs.oms.servicetest.att.OmsClientSimulator;
//import amdocs.oms.servicetest.att.TestService;
//import amdocs.oms.servicetest.att.Utilssss;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.codec.binary.Base64;

import com.amdocs.informationmodel.common.FlexAttr;
import com.amdocs.informationmodel.commonbusinessentities.characteristicvalue.CharacteristicValue;
import com.amdocs.informationmodel.commonbusinessentities.purposedtext.PurposedText;
import com.amdocs.informationmodel.customerdomain.customerorder.RecurringCalculatedPrice;
import com.amdocs.informationmodel.productdomain.productspecification.ProductSpecCharacteristic;
import com.amdocs.informationmodel.productdomain.productspecification.ProductSpecification;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.AllowedAction;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.AssignedBillingOfferDisplay;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.CharacteristicValueDisplay;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ConfigurationRuleMessage;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.NewProductInNewOfferingConfigurationResponse;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductDisplay;
import com.amdocs.services.customerdomain.customerorder.productconfiguration.UpdateProductConfigurationResponse;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.reflection.FieldDictionary;
import com.thoughtworks.xstream.converters.reflection.ImmutableFieldKeySorter;
import com.thoughtworks.xstream.converters.reflection.Sun14ReflectionProvider;
import com.thoughtworks.xstream.io.xml.DomDriver;

import com.amdocs.services.retailapi.*;

//for ban segment 
//import amdocs.oms.servicetest.att.test.*;
import amdocs.oms.sbcapi.*;
//import amdocs.oms.servicetest.att.test.*;
import amdocs.oms.omsinterface.GetBanSegmentRequest;
import amdocs.oms.omsinterface.GetBanSegmentResponse;
import amdocs.oms.omscontext.*;
import amdocs.core.mapping.Mappable.*;

//for sale offer imports
//import com.amdocs.services.salesoffers.retrieveSalesOffers;
import com.amdocs.services.salesoffers.*;
import amdocs.oms.api.*;
//import com.amdocs.services.salesoffers.ejb.*;
//import com.amdocs.services.customerdomain.customerorder.OrderingContext;
import com.amdocs.services.salesoffers.RetrieveCallIntentsAndFiltersRequest;
import com.amdocs.services.salesoffers.RetrieveCallIntentsAndFiltersResponse;
import com.amdocs.services.salesoffers.RetrieveDependenciesResponse;
import com.amdocs.services.salesoffers.RetrieveSalesOffersRequest;
import com.amdocs.services.salesoffers.RetrieveSalesOffersResponse;
import com.amdocs.services.salesoffers.StartPreNegotiationActivityRequest;
import com.amdocs.services.salesoffers.StartPreNegotiationActivityResponse;
import com.amdocs.services.salesoffers.UpdateSalesOfferDispositionRequest;
import com.amdocs.services.salesoffers.UpdateSalesOfferDispositionResponse;
import java.rmi.RemoteException;
import javax.ejb.EJBObject;
import com.amdocs.services.salesoffers.ejb.*;

public class Actions
{
	String SECURITY_CREDENTIALS = "103";
	String SECURITY_PRINCIPAL = "Oms000";
	String login_password = "oms";
	String login_user = "oms";
	String host = "cltv0286.sldc.sbc.com";	
	String port = "9812";
	public static boolean isWAS = true;
	public static Properties properties;
	
	static ApplicationContext aCtx = null;
		
	static OrderingContext cihctx = null;

	static com.amdocs.services.customerdomain.customerorder.OrderingContext pimctx = null;
	static com.amdocs.services.customerdomain.customerorder.OrderingContext pimctxICS = null;
	
	com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvcHome productSvcHome = null;
	com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvc productConfigurationSvcRemote = null;
	
	com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvcHome productSvcHomeICS = null;
	com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvc productConfigurationSvcRemoteICS = null;
	
		
	NegotiationAPIsInterfaceImplHome home = null;
	NegotiationAPIsInterfaceImpl remote = null;	
	
	com.amdocs.services.salesoffers.ejb.SalesOffersSvcHome salesOffersSvcHome=null;
	com.amdocs.services.salesoffers.ejb.SalesOffersSvc osalesOffersSvc = null;
	 
	Object[] myGetBanSegmentInput = null;
	Object[] myRPCInput = null;
	Object[] mySPOInput = null;
	Object[] myRSOInput = null;
	Object[] myRPC_SO_Input = null;
	Object[] myUPCInput = null;
	Object[] myAvailAppInput = null;
	Object[] myUPCSubmitInput = null;
	
	boolean doPrintOutXmls = false;
	

	public int init() throws Throwable {
		
			
		if(!loadProperties())
		{
			return 0;
		}
		if(!initEnvDetails(true))
		{
			return 0;
		}
		

	//	String basePath = "U:\\xml";
		String basePath = "\\\\waboth9hppcew01\\jars\\Conv\\New\\EJB\\xml";
		
		myGetBanSegmentInput = readFile(basePath+"\\pr_getbanseg_in.xml");
		mySPOInput = readFile(basePath+"\\pr_spo_in.xml");
		myRPCInput = readFile(basePath+"\\pr_rpc_in.xml");
		myRSOInput = readFile(basePath+"\\pr_rso_in.xml");
		myRPC_SO_Input = readFile(basePath+"\\pr_rpc_so_in.xml");
		myUPCInput = readFile(basePath+"\\pr_upc_in.xml");
		myAvailAppInput = readFile(basePath+"\\pr_getavailapp_in.xml");
		myUPCSubmitInput = readFile(basePath+"\\pr_upc_sub_in1.xml");
				
		aCtx = new ApplicationContext();
		
		lr.start_transaction("LSPE_RetailAPI_PR_000_Login");
		cihctx = login(null);
		lr.end_transaction("LSPE_RetailAPI_PR_000_Login", lr.AUTO);
			
		System.out.println("Login was sucessful. Security token received is: "+cihctx.getSecurityToken());
		
		
		pimctx= new com.amdocs.services.customerdomain.customerorder.OrderingContext();
		pimctx.setSecurityToken(cihctx.getSecurityToken());
	//	pimctx.setLocaleString(cihctx.getLocale().toLanguageTag());
		pimctx.setSalesChannel("COR");
				
		cihctx.setUserID(cihctx.getSecurityToken());

			Properties p = new Properties();
			Object objHome = null;
	
			p.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY, "com.ibm.websphere.naming.WsnInitialContextFactory");
			p.put(javax.naming.Context.PROVIDER_URL,"corbaloc:iiop:" + host + ":" + port);
			//p.put(javax.naming.Context.PROVIDER_URL,"iiop://" + host + ":" + port);

			p.put(javax.naming.Context.SECURITY_CREDENTIALS, cihctx.getSecurityToken());
			p.put(javax.naming.Context.SECURITY_PRINCIPAL, cihctx.getSecurityToken());
			
			InitialContext _context;
			try 
			{
				lr.start_transaction("LSPE_RetailAPI_PR_005_CreateServices");
				
				_context = new javax.naming.InitialContext(p);
				
				objHome = _context.lookup("com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductConfigurationSvc"); // this line must be here!
				
				
				productSvcHome = (com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvcHome)
								 javax.rmi.PortableRemoteObject.narrow(
										 objHome, 
										 com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvcHome.class);	
				
				productConfigurationSvcRemote = productSvcHome.create();
				
				//get service for get ban segment
				objHome = _context.lookup("/omsserver_websphere/amdocs/oms/sbcapi/NegotiationAPIsInterfaceImpl");
		
  				home = (NegotiationAPIsInterfaceImplHome)PortableRemoteObject.narrow(objHome,NegotiationAPIsInterfaceImplHome.class);
  				
  		
  				remote = (NegotiationAPIsInterfaceImpl)PortableRemoteObject.narrow(home.create(),NegotiationAPIsInterfaceImpl.class);	

  				
  				//get service for retrieve sale offer
  				objHome = _context.lookup("com.amdocs.services.salesoffers.SalesOffersSvc");
    	
    			salesOffersSvcHome = ((SalesOffersSvcHome)PortableRemoteObject.narrow(objHome,SalesOffersSvcHome.class));
    	          
       			osalesOffersSvc = salesOffersSvcHome.create();
       			
       			lr.end_transaction("LSPE_RetailAPI_PR_005_CreateServices", lr.AUTO);
			} 
			catch (NamingException e) 
			{
				e.printStackTrace();
			} 
			catch (RemoteException e) 
			{
				e.printStackTrace();
			} 
			catch (CreateException e)
			{
				e.printStackTrace();
			}
			catch(Exception ex)		
			{
				ex.printStackTrace();
			}
		
	//login and create service for ics
			if(!initEnvDetails(false))
		{
			return 0;
		}
			aCtx = new ApplicationContext();
		
		lr.start_transaction("LSPE_RetailAPI_PR_000_LogintoICS");
		cihctx = login(null);
		lr.end_transaction("LSPE_RetailAPI_PR_000_LogintoICS", lr.AUTO);
			
		System.out.println("Login was sucessful to ICS. Security token received is: "+cihctx.getSecurityToken());
		
		
		pimctxICS= new com.amdocs.services.customerdomain.customerorder.OrderingContext();
		pimctxICS.setSecurityToken(cihctx.getSecurityToken());
	//	pimctxICS.setLocaleString(cihctx.getLocale().toLanguageTag());
		pimctxICS.setSalesChannel("COR");
				
		cihctx.setUserID(cihctx.getSecurityToken());

			p = new Properties();
			objHome = null;
	
			p.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY, "com.ibm.websphere.naming.WsnInitialContextFactory");
			p.put(javax.naming.Context.PROVIDER_URL,"corbaloc:iiop:" + host + ":" + port);

			p.put(javax.naming.Context.SECURITY_CREDENTIALS, cihctx.getSecurityToken());
			p.put(javax.naming.Context.SECURITY_PRINCIPAL, cihctx.getSecurityToken());
			
		
			try 
			{
				lr.start_transaction("LSPE_RetailAPI_PR_005_CreateServicesICS");
				
				_context = new javax.naming.InitialContext(p);
				
				objHome = _context.lookup("com.amdocs.services.customerdomain.customerorder.productconfiguration.ProductConfigurationSvc"); // this line must be here!
				
				
				productSvcHomeICS = (com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvcHome)
								 javax.rmi.PortableRemoteObject.narrow(
										 objHome, 
										 com.amdocs.services.customerdomain.customerorder.productconfiguration.ejb.ProductConfigurationSvcHome.class);	
				
				productConfigurationSvcRemoteICS = productSvcHomeICS.create();
				
       			lr.end_transaction("LSPE_RetailAPI_PR_005_CreateServicesICS", lr.AUTO);
			} 
			catch (NamingException e) 
			{
				e.printStackTrace();
			} 
			catch (RemoteException e) 
			{
				e.printStackTrace();
			} 
			catch (CreateException e)
			{
				e.printStackTrace();
			}
			catch(Exception ex)		
			{
				ex.printStackTrace();
			}
		
		return 0;
	}//end of init


	public int action() throws Throwable {
		
		int th_time = 5;
		String myBAN = lr.eval_string("{BAN}");
	//	myBAN = "401826180";
		
		String convID = UUID.randomUUID().toString().replaceAll("-", "");
		String frameworkID =  UUID.randomUUID().toString().replaceAll("-", "");
		System.out.println("ConvID " + convID);
		System.out.println("frameworkID " + frameworkID);
		pimctx.setConversationIdX9(convID);
		pimctx.setFrameworkId(frameworkID);
		
		pimctxICS.setConversationIdX9(convID);
		pimctxICS.setFrameworkId(frameworkID);
			
		System.out.println(myBAN);
		
		//getban segment
		try
		{
  			
  		GetBanSegmentResponse output = new GetBanSegmentResponse();
  		GetBanSegmentRequest getBanSegmentRequest = (GetBanSegmentRequest) myGetBanSegmentInput[0];
  		BanSegmentRequest myBanRequest = getBanSegmentRequest.getBanSegmentRequest();
  		myBanRequest.setcustomerId(myBAN);
  		getBanSegmentRequest.setBanSegmentRequest(myBanRequest);
  		amdocs.oms.omscontext.Context myCtx = (amdocs.oms.omscontext.Context) myGetBanSegmentInput[1];
	//	System.out.println(Utilssss.serializeObjectToString(myBanRequest));
		lr.start_transaction("LSPE_RetailAPI_PR_010_GetBanSegment");

		output = (GetBanSegmentResponse)remote.getBanSegment(getBanSegmentRequest, myCtx);
		
		lr.end_transaction("LSPE_RetailAPI_PR_010_GetBanSegment", lr.AUTO);
		
		if(doPrintOutXmls)
		System.out.println(Utilssss.serializeObjectToString(output));
		
		output = null;
			
			}
			catch(Exception e)
			{
		        lr.end_transaction("LSPE_RetailAPI_PR_010_GetBanSegment", lr.FAIL);
			    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_010_GetBanSegment");
			    e.printStackTrace();
			}
			finally
			{
				lr.think_time(th_time);
			}
			
		//search product offering
			try
			{
				SearchProductOfferingRequest getRequest = (SearchProductOfferingRequest) mySPOInput[0];
				
				ProductOfferingFilterInfo myFilter = getRequest.getProductOfferingFilterInfoX9();
				SalesRuleContext mySalesContext = myFilter.getSalesRuleContextX9();
				MlCustomerLocal myCustomer = mySalesContext.getCustomerX9();
				myCustomer.setCustomerIDX9(myBAN);
				mySalesContext.setCustomerX9(myCustomer);
				myFilter.setSalesRuleContextX9(mySalesContext);
				getRequest.setProductOfferingFilterInfoX9(myFilter);
				//System.out.println(Utilssss.serializeObjectToString(getRequest));
	     		lr.start_transaction("LSPE_RetailAPI_PR_020_SearchProductOffering");
	     		SearchProductOfferingResponse output = productConfigurationSvcRemote.searchProductOffering(pimctx,getRequest);
			    lr.end_transaction("LSPE_RetailAPI_PR_020_SearchProductOffering", lr.AUTO);
			    
			    if(doPrintOutXmls)
			    System.out.println(Utilssss.serializeObjectToString(output));
			    
				output = null;
			    
			
			}
			catch(Exception e)
			{
				 lr.end_transaction("LSPE_RetailAPI_PR_020_SearchProductOffering", lr.FAIL);
			    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_020_SearchProductOffering");
			    e.printStackTrace();
			}
			finally
			{
				lr.think_time(th_time);
			}
			
		//retrieve product configuration quotation
		try
		{
  		RetrieveProductConfigurationRequest request = (RetrieveProductConfigurationRequest) myRPCInput[0];
  		RetrieveProductConfigurationMask[] mask = (RetrieveProductConfigurationMask[]) myRPCInput[3];
	    RetrieveProductConfigurationFilterInfo filter = (RetrieveProductConfigurationFilterInfo) myRPCInput[1];
	    ProductConfigurationOptions options = (ProductConfigurationOptions) myRPCInput[2];
 	    
		MlCustomerLocal myCustomer = request.getMlCustomerLocalX9();
		myCustomer.setCustomerIDX9(myBAN);
		request.setMlCustomerLocalX9(myCustomer);
			
	    lr.start_transaction("LSPE_RetailAPI_PR_030_RetrieveProductConfig_Quotation");

		RetrieveProductConfigurationResponse output = productConfigurationSvcRemote.retrieveProductConfiguration(pimctx,request,filter,options, mask);
	
		lr.end_transaction("LSPE_RetailAPI_PR_030_RetrieveProductConfig_Quotation", lr.AUTO);
		
		if(doPrintOutXmls)
		System.out.println(Utilssss.serializeObjectToString(output));
		
		output = null;
			
			}
			catch(Exception e)
			{
				 lr.end_transaction("LSPE_RetailAPI_PR_030_RetrieveProductConfig_Quotation", lr.FAIL);
			    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_030_RetrieveProductConfig_Quotation");
			    e.printStackTrace();
			}
			finally
			{
				lr.think_time(th_time);
			}
			
		//retrieve sale offers
		try
		{
			
			RetrieveSalesOffersRequest RSO = (RetrieveSalesOffersRequest) myRSOInput[0];
			MlCustomerLocal myCustomer = RSO.getCustomerX9();
			myCustomer.setCustomerIDX9(myBAN);
			RSO.setCustomerX9(myCustomer);
		 		
			lr.start_transaction("LSPE_RetailAPI_PR_040_RetrievSalesOffers");
		    
			RetrieveSalesOffersResponse output = osalesOffersSvc.retrieveSalesOffers(pimctx,"",RSO);
		  
			lr.end_transaction("LSPE_RetailAPI_PR_040_RetrievSalesOffers", lr.PASS);
			
			if(doPrintOutXmls)
			System.out.println(Utilssss.serializeObjectToString(output));
		
			output = null;
		
      	}
      	catch (Exception e)
      	{
      		    lr.end_transaction("LSPE_RetailAPI_PR_040_RetrievSalesOffers", lr.FAIL);
			    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_040_RetrievSalesOffers");
			    e.printStackTrace();
      		
      	}
      	finally
		{
      		lr.think_time(th_time);
		}
      	
      	//retrieve product configuration sales offer
		try
		{
  		RetrieveProductConfigurationRequest request = (RetrieveProductConfigurationRequest) myRPC_SO_Input[0];
  		RetrieveProductConfigurationMask[] mask = (RetrieveProductConfigurationMask[]) myRPC_SO_Input[3];
	    RetrieveProductConfigurationFilterInfo filter = (RetrieveProductConfigurationFilterInfo) myRPC_SO_Input[1];
	    ProductConfigurationOptions options = (ProductConfigurationOptions) myRPC_SO_Input[2];
 	    
	    MlCustomerLocal myCustomer = request.getMlCustomerLocalX9();
		myCustomer.setCustomerIDX9(myBAN);
		request.setMlCustomerLocalX9(myCustomer);
		
		//System.out.println(Utilssss.serializeObjectToString(request));
		                   
	    lr.start_transaction("LSPE_RetailAPI_PR_050_RetrieveProductConfig_SalesOffer");

		RetrieveProductConfigurationResponse output = productConfigurationSvcRemote.retrieveProductConfiguration(pimctx,request,filter,options, mask);
	
		lr.end_transaction("LSPE_RetailAPI_PR_050_RetrieveProductConfig_SalesOffer", lr.AUTO);
		
		if(doPrintOutXmls)
		System.out.println(Utilssss.serializeObjectToString(output));
		
		output = null;
			
			}
			catch(Exception e)
			{
				 lr.end_transaction("LSPE_RetailAPI_PR_050_RetrieveProductConfig_SalesOffer", lr.FAIL);
			    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_050_RetrieveProductConfig_SalesOffer");
			    e.printStackTrace();
			}
			finally
			{
				lr.think_time(th_time);
			}
		
		//update product config no submit
		try
		{
			//System.out.println(Utilssss.serializeObjectToString(myUPCInput));
			
		
	    
			UpdateProductConfigurationMask[] mask = (UpdateProductConfigurationMask[]) myUPCInput[3];
			UpdateProductConfigurationFilterInfo filter = (UpdateProductConfigurationFilterInfo) myUPCInput[0];
			UpdateProductConfigurationOptions options = (UpdateProductConfigurationOptions) myUPCInput[1];
			UpdateProductConfigurationRequest request = (UpdateProductConfigurationRequest) myUPCInput[2];
			
			ConfigureNewOffering[] myNewOffering = request.getNewOfferingConfiguration();
			myNewOffering[0].setCustomerID(myBAN);
//			myNewOffering[1].setCustomerID(myBAN);
			
			MlCustomerLocal myCustomer = request.getMlCustomerLocalX9();
  			myCustomer.setCustomerIDX9(myBAN);
//  			myCustomer.setAutoPayX9("Y");
//  			myCustomer.setEBillX9("N");
//  			myCustomer.setCreditRiskClassX9("Low");
//  			myCustomer.setUBIndicatorX9("false");
//  			myCustomer.setCustomerSubTypeX9("R");
  			request.setMlCustomerLocalX9(myCustomer);
  			
			request.setNewOfferingConfiguration(myNewOffering);
			
			//System.out.println(Utilssss.serializeObjectToString(filter));
			//System.out.println(Utilssss.serializeObjectToString(options));
			//System.out.println(Utilssss.serializeObjectToString(request));
			//System.out.println(Utilssss.serializeObjectToString(mask));
			
	         lr.start_transaction("LSPE_RetailAPI_PR_060_UpdateProductConfig_NoSubmit");
		
     		// UpdateProductConfigurationResponse output = productConfigurationSvcRemote.updateProductConfiguration(pimctx,filter,options,request, mask);
			UpdateProductConfigurationResponse output = productConfigurationSvcRemoteICS.updateProductConfiguration(pimctxICS,filter,options,request, mask);
			
			 lr.end_transaction("LSPE_RetailAPI_PR_060_UpdateProductConfig_NoSubmit", lr.AUTO);
			    
			if(doPrintOutXmls)
			System.out.println(Utilssss.serializeObjectToString(output));
		
			output = null;
		}
		catch(Exception e)
		{
			 lr.end_transaction("LSPE_RetailAPI_PR_060_UpdateProductConfig_NoSubmit", lr.FAIL);
		    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_060_UpdateProductConfig_NoSubmit");
		    e.printStackTrace();
		}
		finally
		{
			lr.think_time(th_time);
		}
		
//		//get available appointments
		try
			{
				
	
			GetAvailableAppointmentsRequest getAvailAppRequest = (GetAvailableAppointmentsRequest) myAvailAppInput[0];
  			MlCustomerLocal myCustomer = getAvailAppRequest.getMlCustomerLocalX9();
  			myCustomer.setCustomerIDX9(myBAN);
  			ConfigureNewOffering[] myNewOffering = getAvailAppRequest.getNewOfferingConfigurationX9();
  			myNewOffering[0].setCustomerID(myBAN);
  			getAvailAppRequest.setNewOfferingConfigurationX9(myNewOffering);
  			getAvailAppRequest.setMlCustomerLocalX9(myCustomer);
  			
  			//System.out.println(Utilssss.serializeObjectToString(getAvailAppRequest));
  			                   
  			lr.start_transaction("LSPE_RetailAPI_PR_070_GetAvailableApp");

			GetAvailableAppointmentsResponse output = productConfigurationSvcRemote.getAvailableAppointments(pimctx,getAvailAppRequest);
	
			lr.end_transaction("LSPE_RetailAPI_PR_070_GetAvailableApp", lr.AUTO);
			
			if(doPrintOutXmls)
			System.out.println(Utilssss.serializeObjectToString(output));
			
			output = null;
			}
			catch(Exception e)
			{
				lr.end_transaction("LSPE_RetailAPI_PR_070_GetAvailableApp", lr.FAIL);
			    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_070_GetAvailableApp");
			    e.printStackTrace();
			}
			finally
			{
				
			}
			
//		//update product config submit
		try
		{
			UpdateProductConfigurationMask[] mask = (UpdateProductConfigurationMask[]) myUPCSubmitInput[3];
			UpdateProductConfigurationFilterInfo filter = (UpdateProductConfigurationFilterInfo) myUPCSubmitInput[0];
			UpdateProductConfigurationOptions options = (UpdateProductConfigurationOptions) myUPCSubmitInput[1];
			UpdateProductConfigurationRequest request = (UpdateProductConfigurationRequest) myUPCSubmitInput[2];
			
			ConfigureNewOffering[] myNewOffering = request.getNewOfferingConfiguration();
			myNewOffering[0].setCustomerID(myBAN);
		//	myNewOffering[1].setCustomerID(myBAN);
			
			MlCustomerLocal myCustomer = request.getMlCustomerLocalX9();
  			myCustomer.setCustomerIDX9(myBAN);
 			myCustomer.setStrBanX9(myBAN);
//  			myCustomer.setEBillX9("N");
//  			myCustomer.setCreditRiskClassX9("Low");
//  			myCustomer.setUBIndicatorX9("false");
//  			myCustomer.setCustomerSubTypeX9("R");
  			request.setMlCustomerLocalX9(myCustomer);
  			
			request.setNewOfferingConfiguration(myNewOffering);
			//System.out.println(Utilssss.serializeObjectToString(request));
	         lr.start_transaction("LSPE_RetailAPI_PR_080_UpdateProductConfig_Submit");
		
     		 UpdateProductConfigurationResponse output = productConfigurationSvcRemoteICS.updateProductConfiguration(pimctxICS,filter,options,request, mask);
		//	UpdateProductConfigurationResponse output = productConfigurationSvcRemote.updateProductConfiguration(pimctx,filter,options,request, mask);
			 lr.end_transaction("LSPE_RetailAPI_PR_080_UpdateProductConfig_Submit", lr.AUTO);
			    
	//		if(doPrintOutXmls)
//			String myXML = Utilssss.serializeObjectToString(output);
//		 File newTextFile = new File("C:\\1thetextfile.txt");
//
//            java.io.FileWriter fw = new java.io.FileWriter(newTextFile);
//            fw.write(myXML);
//            fw.close();
//		System.out.println(Utilssss.serializeObjectToString(output));
//			System.out.println();
		
			output = null;
		}
		catch(Exception e)
		{
			 lr.end_transaction("LSPE_RetailAPI_PR_080_UpdateProductConfig_Submit", lr.FAIL);
		    System.out.println("Exception while calling service: LSPE_RetailAPI_PR_080_UpdateProductConfig_Submit");
		    e.printStackTrace();
		}
		finally
		{
			lr.think_time(th_time);
		}
		
		return 0;

	}//end of action

	public int end() throws Throwable {
		
		
		return 0;
	}//end of end
	
	
	private static boolean loadProperties() {
		InputStream inputStream = null;
		try {
				inputStream = new FileInputStream("config.properties");
				properties = new Properties();
				properties.load(inputStream);
				return true;
			
	} catch (Exception e) {
		System.out.println("Exception: " + e);
		return false;
	} finally {
		try {
			inputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		
	}
//	
	private Object[] readFile(String pathToUnput)
	{
		if(pathToUnput == null)
			return null;
		File f = new File(pathToUnput);
		if(!f.exists() || f.isDirectory())
		{
			System.err.println("there is no file located at: " + pathToUnput);
			return null;
		}
		return (Object[])Utilssss.DEserializeObjectFromString(f);
		
	}
	/**
	 *  Login to the OMS.
	 *  This method get a valid ticket from UAMS and set the Locale.
	 *  @return an OrderingContext object with SecurityToken and locale.
	 */
	public OrderingContext login(String iUserID)
	{
		if(true)
		{
			Hashtable env = new Hashtable(2); 
			//env.put("org.omg.CORBA.ORBClass", "com.ibm.CORBA.iiop.ORB");
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.ibm.websphere.naming.WsnInitialContextFactory"); 
			env.put(Context.PROVIDER_URL, "iiop://"+host+":"+port+""); 
			env.put(javax.naming.Context.SECURITY_CREDENTIALS,SECURITY_CREDENTIALS);
			env.put(javax.naming.Context.SECURITY_PRINCIPAL, SECURITY_PRINCIPAL);
			InitialContext ctx;
			try {
				ctx = new InitialContext(env);
			
				Object obj = ctx.lookup(UamsEjbBasicLoginHome.JNDI_NAME); 
				
				UamsEjbBasicLoginHome home = (UamsEjbBasicLoginHome) PortableRemoteObject.narrow (obj, UamsEjbBasicLoginHome.class);
				UamsEjbBasicLogin remote= (UamsEjbBasicLogin)PortableRemoteObject.narrow(home.create(), UamsEjbBasicLogin.class);
				
				
				
				
				//char[] password = { 'A', 's', 'm', 's', 'a' ,'1'};
				char[] password = login_password.toCharArray();

				OrderingContext ctx2 = new OrderingContext();
				
				ctx2.setSecurityToken(remote.login(login_user,password ,"UIF"));
				
				//ctx2.setSecurityToken(remote.login("OMS",password,"Oms111"));
				ctx2.setLocale(Locale.ENGLISH);
				
				return ctx2;
			}
			catch (Exception e) 
			{
				e.printStackTrace();
				return null;
			} 	
		}

		//-Damdocs.uams.config.resource=res/gen/client
		//-DSEC_SRV_CONN=localhost:7001


		UamsLoginContext loginCtx = null;
		amdocs.uamsimpl.client.login.direct.DirectLoginServiceWrapper wrapper;
		OrderingContext ctx = new OrderingContext();
		
		Random generator = new Random();
		int r = generator.nextInt(1000000);
		
		String userID = "T"+r;
		if (iUserID != null && iUserID.trim().length() > 0)
		{
			userID = iUserID;
		}
		
		try 
		{
			// Returns UamsService according to service bind name, in this case a direct login service 
			wrapper =  (amdocs.uamsimpl.client.login.direct.DirectLoginServiceWrapper)UamsSystem.getService(null, UamsSystem.LN_UAMS_DIRECT_LOGIN);
			//returns the login context which contains the security ticket - String user, String password, String evironment, String applicationId
			System.out.println("UAMS Login, userID:=" + userID);
			loginCtx = wrapper.login(login_user, login_password, "UIF", "oms");
			
			//InteractiveLoginService ct = (InteractiveLoginService)UamsSystem.getService(null, UamsSystem.LN_UAMS_LOGIN);
			//loginCtx = ct.login(loginCtx);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println();
			System.out.println("Need to define in run parameters bellow parameters:");
			System.out.println("-Damdocs.uams.config.resource=res/gen/client");
			System.out.println("-DSEC_SRV_CONN=localhost:7001");
			System.exit(-1);
		}
		
		ctx.setSecurityToken(loginCtx.getTicket());
		ctx.setLocale(Locale.ENGLISH);
		return ctx;
	}
//		
	private boolean initEnvDetails(boolean isAPI)
	{
		if(properties == null)
		{
			System.out.println("Error: config.properties file does not exist. Terminating the program");
			return false;
		}
			String definedHost = null;
		
		String definedPort = null;
		
		if(isAPI)
		{
			definedHost =  properties.getProperty("HOST");
		}
			else
		{
			definedHost =  properties.getProperty("HOST1");
		}
		
		if(isAPI)
		{
			definedPort = properties.getProperty("PORT");
		}
		else
		{
			definedPort = properties.getProperty("PORT1");
		}
		
		
		if(definedHost == null || definedPort == null)
		{
			System.out.println("Host or port are not defined in config.properties file. Terminating the program");
			return false;
		}
		
		host = definedHost;
		port = definedPort;

		System.out.println("Host: "+host);
		System.out.println("Port: "+port);
		String wasOrWeblogic = properties.getProperty("SERVER");
		if(wasOrWeblogic == null)
		{
			System.out.println("SERVER value is not defined config.properties file. Terminating the program");
			return false;
		}
		if("weblogic".equals(wasOrWeblogic))
		{
			isWAS = false;
		}
		
		System.out.println("Working on server: "+wasOrWeblogic);
		String securityCredentials = properties.getProperty("WEBSPHERE_PASSWORD");
		String securityPrincipal = properties.getProperty("WEBSPHERE_USER");
		if(securityCredentials == null || securityPrincipal == null)
		{
			System.out.println("Websphere user or password are not defined in " +
					"config.properties file. Terminating the program");
			return false;
		}

		SECURITY_CREDENTIALS = securityCredentials;
		SECURITY_PRINCIPAL = securityPrincipal;
		System.out.println("Websphere user: "+SECURITY_PRINCIPAL);
		System.out.println("Websphere password: "+SECURITY_CREDENTIALS);
		
		
		String loginUser = properties.getProperty("LOGIN_USER");
		String loginPassword = properties.getProperty("LOGIN_PASSWORD");
		if(loginPassword == null || loginUser == null)
		{
			System.out.println("login user or password are not defined in " +
					"config.properties file. Terminating the program");
			return false;
		}

		login_password = loginPassword;
		login_user = loginUser;
		System.out.println("Login user: "+login_user);
		System.out.println("Login password: "+login_password);
		System.out.println("Path to input and output files: "+properties.getProperty("OUTPUT_PATH"));
		
		return true;
		
	}

}

