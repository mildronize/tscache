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

import net.opentsdb.core.DataPoints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache fragment
 * @since 2.0
 */

final class CacheFragment{

  private final HttpQuery query;
  private ArrayList<DataPoints[]> dataPoints;
  private boolean exist;
  private Exception exception;

  public CacheFragment(final HttpQuery query){
    this.query = query;
    this.exist = false;
    this.dataPoints = null;
    this.exception = null;
  }

  public CacheFragment(final HttpQuery query, boolean exist){
    this.query = query;
    this.exist = exist;
    this.dataPoints = null;
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

  public void addDataPoints(ArrayList<DataPoints[]> dataPoints){ this.dataPoints.addAll(dataPoints); }

  public ArrayList<DataPoints[]> getDataPoints(){
    return dataPoints;
  }
  //final  ArrayList<DataPoints[]> dataPoints, final boolean exist

}
