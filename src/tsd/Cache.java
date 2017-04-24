// This file is part of my thesis
// Copyright (C) 2017  Thada Wangthammang
//
// This file extends OpenTSDB license following below.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.

package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.opentsdb.core.DataPoints;

import net.opentsdb.core.TSQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.tsd.cache.QueryFragment;
import net.opentsdb.tsd.cache.RPCClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache module
 * @since 2.0
 */

final class Cache{
  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  private ArrayList<CacheFragment> fragments;
  private String cacheServerHost = "docker";
  private TSQuery ts_query;

  public Cache(final TSQuery ts_query){
    LOG.debug("Create Cache object");
    this.ts_query = ts_query;
    fragments = new ArrayList<CacheFragment>();

    // Find missing cache and mark which fragment is exist?
    makeFragments();
  }

  static String callRPC(final String cacheServerHost, final String input, final String rpcName){
    RPCClient rpcClient = null;
    String response = null;
    try {
      rpcClient = new RPCClient(cacheServerHost, rpcName);
      response = rpcClient.call(input);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (rpcClient != null)
        try {
          rpcClient.close();
        } catch (IOException e) {
          LOG.error("RPCClient Exception: " + e);
        }
    }
    return response;
  }

  static String buildSubQueryRPC(final TSQuery ts_query){
    /*
    Metric will be stored in TSSubQuery (A TSQuery must be at least 1 TSSubQuery) )

    Limitation:
    - This case uses only one TSSubQuery.
    - Need to know both of metric and all tags to identify which time series (literal_or)
     */
    StringBuilder tmp = new StringBuilder();
    // metric
    tmp.append(ts_query.getQueries().get(0).getMetric());
    tmp.append('-');
    // tags
    for(TagVFilter filter : ts_query.getQueries().get(0).getFilters() ) {
      if (filter.getType().equals("literal_or")) {
        tmp.append(filter.getTagk());
        tmp.append('=');
        tmp.append(filter.getFilter());
      }else {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Bad request",
          "Cache Error: Need to exactly know both of metric and all tags to identify");
      }
      tmp.append(",");
    }

    tmp.append(":");
    //start_time
    tmp.append(ts_query.startTime());
    tmp.append("-");
    tmp.append(ts_query.endTime());
    return tmp.toString();
  }

  private void makeFragments(){
    /*
    [metric]-[key1=value1,key2=value2,...]:[start_time]-[end_time]
     */
    String response = callRPC(cacheServerHost, buildSubQueryRPC(ts_query), "tsdb-cache-query-fragments");
    try {
      QueryFragment[] queryFragments = new ObjectMapper().readValue( response, QueryFragment[].class);
      for (QueryFragment queryFragment : queryFragments) {
        fragments.add(
          new CacheFragment(makeSubTSQuery(queryFragment.getStart_time(), queryFragment.getEnd_time()), queryFragment.isCached())
        );
      }
    }catch (Exception e){
      LOG.error("QueryFragments ObjectMapper Exception: " + e);
    }

  }

  private TSQuery makeSubTSQuery(final long start_time, final long end_time) {
    TSQuery sub_query= new TSQuery();
    sub_query.clone(ts_query);
    // set a peroid of time
    sub_query.setStartTime(start_time);
    sub_query.setEndTime(end_time);
    try {
      LOG.debug("SubQuery : " + sub_query.toString());
      sub_query.validateAndSetQuery();
    } catch (Exception e) {
      LOG.error("makeSubTSQuery Exception: " + e);
    }
    return sub_query;
  }

  ArrayList<CacheFragment> getFragments(){
    return this.fragments;
  }

}
