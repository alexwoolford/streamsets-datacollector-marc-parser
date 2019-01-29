/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.woolford.stage.processor.marcparser;

import com.streamsets.pipeline.api.Field;
import io.woolford.stage.lib.marcparser.Errors;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.apache.commons.lang3.StringUtils;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.ControlFieldImpl;
import org.marc4j.marc.impl.DataFieldImpl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public abstract class MarcParserProcessor extends SingleLaneRecordProcessor {

  /**
   * Gives access to the UI configuration of the stage provided by the {@link MarcParserDProcessor} class.
   */
  public abstract String getConfig();

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    if (getConfig().equals("invalidValue")) {
      issues.add(
          getContext().createConfigIssue(
              Groups.MARC_PARSER.name(), "config", Errors.MARC_PARSER_00, "Here's what's wrong..."
          )
      );
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  public MarcParserProcessor() {
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

    String mrc =  record.get("/text").getValue().toString();
    InputStream is = new ByteArrayInputStream( mrc.getBytes(StandardCharsets.UTF_8 ) );
    MarcReader reader = new MarcStreamReader(is);

    while (reader.hasNext()) {
      org.marc4j.marc.Record mrcRecord = reader.next();
      List controlFields = mrcRecord.getControlFields();
      for (Object controlField : controlFields) {
        String tag = "/" + ((ControlFieldImpl) controlField).getTag();
        String data = ((ControlFieldImpl) controlField).getData();
        record.set(tag, Field.create(data));
      }

      List dataFields = mrcRecord.getDataFields();
      for (Object dataField : dataFields) {
        String tag = "/" + ((DataFieldImpl) dataField).getTag();
        List<Subfield> subFields = ((DataFieldImpl) dataField).getSubfields();
        String data = StringUtils.join(subFields, " ");
        record.set(tag, Field.create(data));
      }

    }

    batchMaker.addRecord(record);
  }

}
