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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.DataPoints;

import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.query.expression.ExpressionTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache fragment
 * @since 2.0
 */

// TODO: Split into 2 classes, for taking result between functions and for cache fragment purpose
  /*
  First class contains `dataPoints`, `exception` and `annotations`, otherwise for second class
   */

final class CacheFragment{

  private static final Logger LOG = LoggerFactory.getLogger(CacheFragment.class);

  private final HttpQuery query;
  private ArrayList<DataPoints[]> dataPoints;
  private List<Annotation> annotations;
  private boolean exist;
  private Exception exception;

  public CacheFragment(final HttpQuery query){
    this.query = query;
    this.exist = false;
    this.dataPoints = null;
    this.annotations = null;
    this.exception = null;
  }

  public CacheFragment(final HttpQuery query, boolean exist){
    this.query = query;
    this.exist = exist;
    this.dataPoints = null;
    this.annotations = null;
    this.exception = null;
  }

  public boolean isExist(){
    return exist;
  }

  public HttpQuery getQuery(){
    return query;
  }

  public Exception getException(){
    return exception;
  }

  public void setException(Exception exception){
    this.exception = exception;
  }

  public void setDataPoints(ArrayList<DataPoints[]> dataPoints){ this.dataPoints = dataPoints; }

  public void addDataPoints(List<DataPoints[]> dataPoints){ this.dataPoints.addAll(dataPoints); }

  public ArrayList<DataPoints[]> getDataPoints(){
    return dataPoints;
  }

  public List<Annotation> getAnnotations(){
    return annotations;
  }
  public void setAnnotations(List<Annotation> annotations){ this.annotations = annotations; }

  public void addAnnotations(List<Annotation> annotations){ this.annotations.addAll(annotations); }
  //final  ArrayList<DataPoints[]> dataPoints, final boolean exist

  /**
   * Processing for a data point sub query
   * @param tsdb The TSDB to which we belong
   * @param data_query TSQuery which be parsed
   */
  public Deferred<Object> processSubQueryAsync(final TSDB tsdb, final TSQuery data_query) throws Exception{

    final int nqueries = data_query.getQueries().size();
    final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(nqueries);
    final List<Annotation> globals = new ArrayList<Annotation>();

    // Perform cache fragment for sending to deferred object
    final CacheFragment cacheFragment_result = new CacheFragment(query);
    this.setDataPoints(new ArrayList<DataPoints[]>());
    this.setAnnotations(new ArrayList<Annotation>());

    class ErrorCB implements Callback<Object, Exception> {
      public Object call(final Exception e) throws Exception {
        LOG.error("SubQuery exception: ", e);
        exception = e;
        return null;
      }
    }

    /**
     * After all of the queries have run, we get the results in the order given
     * and add dump the results in an array
     */
    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
      public Object call(final ArrayList<DataPoints[]> query_results)
        throws Exception {
          dataPoints.addAll(query_results);
          LOG.debug("Got datapoints");
          return null;
      }
    }

    /**
     * Callback executed after we have resolved the metric, tag names and tag
     * values to their respective UIDs. This callback then runs the actual
     * queries and fetches their results.
     */
    class BuildCB implements Callback<Object, Query[]> {
      @Override
      public Object call(final Query[] queries) {
        final ArrayList<Deferred<DataPoints[]>> deferreds =
          new ArrayList<Deferred<DataPoints[]>>(queries.length);
        for (final Query query : queries) {
          deferreds.add(query.runAsync());
        }
        return Deferred.groupInOrder(deferreds).addCallback(new QueriesCB());
      }
    }

    /** Handles storing the global annotations after fetching them */
    class GlobalCB implements Callback<Object, List<Annotation>> {
      public Object call(final List<Annotation> annotations) throws Exception {
        //globals.addAll(annotations);
        cacheFragment_result.addAnnotations(globals);
        return data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB());
      }
    }
    LOG.debug("Starting processSubQueryAsync");
    return data_query.buildQueriesAsync(tsdb)
      .addCallback(new BuildCB())
      .addErrback(new ErrorCB());
  }

}
