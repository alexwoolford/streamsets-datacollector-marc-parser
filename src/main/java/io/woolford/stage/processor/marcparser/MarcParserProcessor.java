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
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import io.woolford.stage.lib.marcparser.Errors;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Subfield;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public abstract class MarcParserProcessor extends SingleLaneRecordProcessor {

  /**
   * Gives access to the UI configuration of the stage provided by the {@link MarcParserDProcessor} class.
   */
  public abstract String getConfig();

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  public MarcParserProcessor() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

    String mrc =  record.get("/text").getValue().toString();
    try (
        InputStream is = new ByteArrayInputStream(mrc.getBytes(StandardCharsets.UTF_8))) {
    MarcReader reader = new MarcStreamReader(is);
      int recordNumber = 0;
    while (reader.hasNext()) {
        Record bibRecord = getContext().createRecord(record, "-" + recordNumber++);
        bibRecord.set(Field.createListMap(new LinkedHashMap<String, Field>()));
      org.marc4j.marc.Record mrcRecord = reader.next();
        bibRecord.set("/leader", Field.create(mrcRecord.getLeader().marshal()));
        List<ControlField> marcControlFields = mrcRecord.getControlFields();
        for (ControlField marcControlField : marcControlFields) {
          String tag = "/" + marcControlField.getTag();
          String data = marcControlField.getData();
          bibRecord.set(tag, Field.create(data));
        }
        List<DataField> marcDataFields = mrcRecord.getDataFields();
        LinkedHashMap<String, List<Field>> dataFields = new LinkedHashMap<>();
        for (DataField marcDataField : marcDataFields) {
          String tag = "/" + marcDataField.getTag();
          if (!dataFields.containsKey(tag)) {
            dataFields.put(tag, new ArrayList<Field>());
      }
          List<Field> dataField = dataFields.get(tag);
          LinkedHashMap<String, Field> mapOfDataField = MarcParserProcessor.getMapOfDataField(marcDataField);
          dataField.add(Field.createListMap(mapOfDataField));
        }
        dataFields.forEach((key, fields1) -> bibRecord.set(key, Field.create(fields1)));
        batchMaker.addRecord(bibRecord);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

      }

  /**
   * Exposed for testing
   * Converts a MARC datafield into a map of String:Field.
   * Keys are
   * <ul><li>indicator1</li><li>indicator2</li><li>each unique code is another key</li></ul>
   * Values are
   * <ul><li>String for indicators (as a {@link Field})</li>
   * <li>{@link List}of {@link String} (also as a {@link Field})</li></ul>
   * <p> for the following example</p>
   * <code><pre>
   *    650 #0 $a Theater
   *           $z United States
   *           $v Biography
   *           $v Dictionaries
   *  </pre></code>
   * This method will produce a map with 5 keys, as json
   * <code><pre>
   *  {
   *    "indicator1":" ",
   *    "indicator2":"0",
   *    "a":["Theater"],
   *    "z":["United States"],
   *    "v":["Bibliography","Dictionaries"]
   *  }
   *  </pre></code>
   * <p>The indicators are single values as strings, the 'a' and 'z' codes are lists of strings with a single entry, the 'v' code is a list of strings with two entries  </p>
   *
   * @param marcDataField The MARC datafield to parse
   * @return The list map of data in this datafield
   */
  static LinkedHashMap<String, Field> getMapOfDataField(DataField marcDataField) {
    LinkedHashMap<String, Field> recordDataFields = new LinkedHashMap<>();
    recordDataFields.put("indicator1", Field.create(marcDataField.getIndicator1()));
    recordDataFields.put("indicator2", Field.create(marcDataField.getIndicator2()));
    LinkedHashMap<String, List<Field>> subFields = new LinkedHashMap<String, List<Field>>();
    for (Subfield subField : marcDataField.getSubfields()) {
      String code = Character.toString(subField.getCode());
      if (!subFields.containsKey(code)) {
        subFields.put(code, new ArrayList<Field>());
    }
      List<Field> subFieldList = subFields.get(code);
      subFieldList.add(Field.create(subField.getData()));
    }
    subFields.forEach((key, fields1) -> recordDataFields.put(key, Field.create(fields1)));
    return recordDataFields;
  }

}
