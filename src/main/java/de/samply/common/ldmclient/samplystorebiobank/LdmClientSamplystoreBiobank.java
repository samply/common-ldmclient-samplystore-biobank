package de.samply.common.ldmclient.samplystorebiobank;

import de.samply.common.ldmclient.LdmClientException;
import de.samply.common.ldmclient.LdmClientUtil;
import de.samply.common.ldmclient.LdmClientView;
import de.samply.common.ldmclient.model.LdmQueryResult;
import de.samply.share.model.bbmri.BbmriResult;
import de.samply.share.model.common.Error;
import de.samply.share.model.common.QueryResultStatistic;
import de.samply.share.model.common.View;
import de.samply.share.utils.QueryConverter;
import java.io.IOException;
import javax.xml.bind.JAXBException;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Client to communicate with the local datamanagement implementation "Samply Store Rest".
 */
public class LdmClientSamplystoreBiobank extends
    LdmClientView<BbmriResult, de.samply.share.model.osse.QueryResultStatistic,
        de.samply.share.model.osse.Error, de.samply.share.model.osse.View> {

  private static final String REST_INFO = "info";

  public LdmClientSamplystoreBiobank(CloseableHttpClient httpClient, String ldmBaseUrl,
      boolean useCaching) throws LdmClientException {
    this(httpClient, ldmBaseUrl, useCaching, CACHE_DEFAULT_SIZE);
  }

  public LdmClientSamplystoreBiobank(CloseableHttpClient httpClient, String ldmBaseUrl,
      boolean useCaching, int cacheSize) throws LdmClientException {
    super(httpClient, ldmBaseUrl, useCaching, cacheSize);
  }

  public boolean isLdmSamplystoreBiobank() {
    return true;
  }

  @Override
  protected Class<de.samply.share.model.osse.View> getSpecificViewClass() {
    return de.samply.share.model.osse.View.class;
  }

  @Override
  protected Class<BbmriResult> getResultClass() {
    return BbmriResult.class;
  }

  @Override
  protected Class<de.samply.share.model.osse.QueryResultStatistic> getStatisticsClass() {
    return de.samply.share.model.osse.QueryResultStatistic.class;
  }

  @Override
  protected Class<de.samply.share.model.osse.Error> getErrorClass() {
    return de.samply.share.model.osse.Error.class;
  }

  @Override
  protected Class<?> getObjectFactoryClassForPostView() {
    // TODO: Check why both ObjectFactories differ (osse vs bbmri)
    return de.samply.share.model.osse.ObjectFactory.class;
  }

  @Override
  protected Class<?> getObjectFactoryClassForResult() {
    return de.samply.share.model.bbmri.ObjectFactory.class;
  }

  @Override
  protected de.samply.share.model.osse.View convertCommonViewToSpecificView(View view)
      throws JAXBException {
    return QueryConverter.convertCommonViewToOsseView(view);
  }

  @Override
  protected View convertSpecificViewToCommonView(de.samply.share.model.osse.View osseView)
      throws JAXBException {
    return QueryConverter.convertOsseViewToCommonView(osseView);
  }

  @Override
  protected LdmQueryResult convertQueryResultStatisticToCommonQueryResultStatistic(
      de.samply.share.model.osse.QueryResultStatistic qrs) throws JAXBException {
    QueryResultStatistic queryResultStatistic = QueryConverter
        .convertOsseQueryResultStatisticToCommonQueryResult(qrs);

    return new LdmQueryResult(queryResultStatistic);
  }

  @Override
  protected LdmQueryResult convertSpecificErrorToCommonError(de.samply.share.model.osse.Error error)
      throws JAXBException {
    Error convertedError = QueryConverter.convertOsseErrorToCommonError(error);

    return new LdmQueryResult(convertedError);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BbmriResult getResult(String location) throws LdmClientException {
    BbmriResult queryResult = new BbmriResult();
    QueryResultStatistic queryResultStatistic = getQueryResultStatistic(location);

    if (queryResultStatistic != null && queryResultStatistic.getTotalSize() > 0) {
      for (int i = 0; i < queryResultStatistic.getNumberOfPages(); ++i) {
        BbmriResult queryResultPage = getResultPage(location, i);
        queryResult.setDirectoryCollectionId(queryResultPage.getDirectoryCollectionId());
        queryResult.setNegotiatorQueryId(queryResultPage.getNegotiatorQueryId());
        queryResult.setNumberOfPatients(queryResultPage.getNumberOfPatients());
        queryResult.setNumberOfSpecimens(queryResultPage.getNumberOfSpecimens());
        queryResult.setDonors(queryResultPage.getDonors());
      }
      queryResult.setQueryId(Integer.parseInt(queryResultStatistic.getRequestId()));
    }
    return queryResult;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getVersionString() {
    HttpGet httpGet = new HttpGet(LdmClientUtil.addTrailingSlash(getLdmBaseUrl()) + REST_INFO);
    try (CloseableHttpResponse response = getHttpClient().execute(httpGet)) {
      Header versionHeader = response.getFirstHeader("version");
      if (versionHeader != null) {
        return versionHeader.getValue();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return "unknown";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getUserAgentInfo() {
    // TODO: Check how this is provided via samply store
    return "samply.store/" + getVersionString();
  }
}
