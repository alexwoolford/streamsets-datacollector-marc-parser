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
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.DataFieldImpl;
import org.marc4j.marc.impl.SubfieldImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TestMarcParserProcessor {
  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws StageException, IOException {
    String marc = new String(Files.readAllBytes(Paths.get("src/test/resources/summerland.mrc")));
    ProcessorRunner runner = new ProcessorRunner.Builder(MarcParserDProcessor.class)
        .addConfiguration("config", "value")
        .addOutputLane("output")
        .build();

    runner.runInit();

    try {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("text", Field.create(marc));
      record.set(Field.create(fields));

      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      assertEquals(1, output.getRecords().get("output").size());
      Record record1 = output.getRecords().get("output").get(0);
      assertEquals(Field.Type.LIST_MAP, record1.get().getType());
      Field leader = record1.get("/leader");
      assertEquals("00714cam a2200205 a 4500", leader.getValueAsString());
      Field _001 = record1.get("/001");
      assertEquals("12883376", _001.getValueAsString());
      Field _005 = record1.get("/005");
      assertEquals("20030616111422.0", _005.getValueAsString());
      Field _008 = record1.get("/008");
      assertEquals("020805s2002    nyu    j      000 1 eng  ", _008.getValueAsString());

      // Fields 020
      // 020   $a0786808772
      // 020   $a0786816155 (pbk.)
      Field _020 = record1.get("/020");
      assertEquals(Field.Type.LIST, _020.getType());
      List<Field> _020List = _020.getValueAsList();
      assertEquals(2, _020List.size());
      assertSame(record1.get("/020[0]"), _020List.get(0));
      Field _020_0 = _020List.get(0);
      assertEquals(Field.Type.LIST_MAP, _020_0.getType());
      assertEquals(' ', record1.get("/020[0]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/020[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/020[0]/a").getValueAsList().size());
      assertEquals("0786808772", record1.get("/020[0]/a[0]").getValueAsString());
      assertEquals(' ', record1.get("/020[1]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/020[1]/indicator2").getValueAsChar());
      assertEquals("0786816155 (pbk.)", record1.get("/020[1]/a[0]").getValueAsString());

      //Field 040
      // 040   $aDLC$cDLC$dDLC
      assertEquals(1, record1.get("/040").getValueAsList().size());
      assertEquals(' ', record1.get("/040[0]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/040[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/040[0]/a").getValueAsList().size());
      assertEquals("DLC", record1.get("/040[0]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/040[0]/c").getValueAsList().size());
      assertEquals("DLC", record1.get("/040[0]/c[0]").getValueAsString());
      assertEquals(1, record1.get("/040[0]/d").getValueAsList().size());
      assertEquals("DLC", record1.get("/040[0]/d[0]").getValueAsString());

      //Field 100
      //100 1 $aChabon, Michael.
      assertEquals(1, record1.get("/100").getValueAsList().size());
      assertEquals('1', record1.get("/100[0]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/100[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/100[0]/a").getValueAsList().size());
      assertEquals("Chabon, Michael.", record1.get("/100[0]/a[0]").getValueAsString());

      //Field 245
      // 245 10$aSummerland /$cMichael Chabon.
      assertEquals(1, record1.get("/245").getValueAsList().size());
      assertEquals('1', record1.get("/245[0]/indicator1").getValueAsChar());
      assertEquals('0', record1.get("/245[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/245[0]/a").getValueAsList().size());
      assertEquals("Summerland /", record1.get("/245[0]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/245[0]/c").getValueAsList().size());
      assertEquals("Michael Chabon.", record1.get("/245[0]/c[0]").getValueAsString());

      //Fields 650
      // 650  1$aFantasy.
      // 650  1$aBaseball$vFiction.
      // 650  1$aMagic$vFiction.
      assertEquals(3, record1.get("/650").getValueAsList().size());
      assertEquals(' ', record1.get("/650[0]/indicator1").getValueAsChar());
      assertEquals('1', record1.get("/650[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/650[0]/a").getValueAsList().size());
      assertEquals("Fantasy.", record1.get("/650[0]/a[0]").getValueAsString());
      assertEquals(' ', record1.get("/650[1]/indicator1").getValueAsChar());
      assertEquals('1', record1.get("/650[1]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/650[1]/a").getValueAsList().size());
      assertEquals("Baseball", record1.get("/650[1]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/650[1]/v").getValueAsList().size());
      assertEquals("Fiction.", record1.get("/650[1]/v[0]").getValueAsString());
      assertEquals(' ', record1.get("/650[2]/indicator1").getValueAsChar());
      assertEquals('1', record1.get("/650[2]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/650[2]/a").getValueAsList().size());
      assertEquals("Magic", record1.get("/650[2]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/650[2]/v").getValueAsList().size());
      assertEquals("Fiction.", record1.get("/650[2]/v[0]").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testGetMapOfDataField() {
    DataField field = new DataFieldImpl("650", ' ', '0');
    Subfield a = new SubfieldImpl('a', "Theater");
    field.addSubfield(a);
    Subfield z = new SubfieldImpl('z', "United States");
    field.addSubfield(z);
    Subfield v0 = new SubfieldImpl('v', "Biography");
    field.addSubfield(v0);
    Subfield v1 = new SubfieldImpl('v', "Dictionaries.");
    field.addSubfield(v1);
    LinkedHashMap<String, Field> mapOfDataField = MarcParserProcessor.getMapOfDataField(field);
    assertEquals(' ', mapOfDataField.get("indicator1").getValue());
    assertEquals('0', mapOfDataField.get("indicator2").getValue());
    assertEquals(Field.Type.LIST, mapOfDataField.get("a").getType());
    List<Field> aSubfield = mapOfDataField.get("a").getValueAsList();
    assertEquals(1, aSubfield.size());
    assertEquals("Theater", aSubfield.get(0).getValueAsString());
    List<Field> vSubfield = mapOfDataField.get("v").getValueAsList();
    assertEquals(2, vSubfield.size());
    assertEquals("Biography", vSubfield.get(0).getValueAsString());
    assertEquals("Dictionaries.", vSubfield.get(1).getValueAsString());
  }

  @Test
  public void testProcessMultiRecords() throws StageException, IOException {
    String marc = new String(Files.readAllBytes(Paths.get("src/test/resources/chabon.mrc")));

    ProcessorRunner runner = new ProcessorRunner.Builder(MarcParserDProcessor.class)
        .addConfiguration("config", "value")
        .addOutputLane("output")
        .build();

    runner.runInit();
    //Not sure what groups or the labels are used for, but we should make sure we don't break tests
    String groupLabel=Groups.MARC_PARSER.getLabel();
    assertEquals("MARC Parser",groupLabel);

    try {
      Record record = RecordCreator.create();
      Map<String, Field> fields = new HashMap<>();
      fields.put("text", Field.create(marc));
      record.set(Field.create(fields));

      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      assertEquals(2, output.getRecords().get("output").size());
      Record record0 = output.getRecords().get("output").get(0);
      assertEquals(Field.Type.LIST_MAP, record0.get().getType());
      Field leader0 = record0.get("/leader");
      assertEquals("00759cam a2200229 a 4500", leader0.getValueAsString());
      Field _001 = record0.get("/001");
      assertEquals("11939876", _001.getValueAsString());
      Field _005 = record0.get("/005");
      assertEquals("20041229190604.0", _005.getValueAsString());
      Field _008 = record0.get("/008");
      assertEquals("000313s2000    nyu           000 1 eng  ", _008.getValueAsString());

      assertEquals(' ', record0.get("/020[0]/indicator1").getValueAsChar());
      assertEquals(' ', record0.get("/020[0]/indicator2").getValueAsChar());
      assertEquals(1, record0.get("/020[0]/a").getValueAsList().size());
      assertEquals("0679450041 (acid-free paper)", record0.get("/020[0]/a[0]").getValueAsString());

      //Field 040
      // 040   $aDLC$cDLC$dDLC
      assertEquals(1, record0.get("/040").getValueAsList().size());
      assertEquals(' ', record0.get("/040[0]/indicator1").getValueAsChar());
      assertEquals(' ', record0.get("/040[0]/indicator2").getValueAsChar());
      assertEquals(1, record0.get("/040[0]/a").getValueAsList().size());
      assertEquals("DLC", record0.get("/040[0]/a[0]").getValueAsString());
      assertEquals(1, record0.get("/040[0]/c").getValueAsList().size());
      assertEquals("DLC", record0.get("/040[0]/c[0]").getValueAsString());
      assertEquals(1, record0.get("/040[0]/d").getValueAsList().size());
      assertEquals("DLC", record0.get("/040[0]/d[0]").getValueAsString());

      //Field 100
      //100 1 $aChabon, Michael.
      assertEquals(1, record0.get("/100").getValueAsList().size());
      assertEquals('1', record0.get("/100[0]/indicator1").getValueAsChar());
      assertEquals(' ', record0.get("/100[0]/indicator2").getValueAsChar());
      assertEquals(1, record0.get("/100[0]/a").getValueAsList().size());
      assertEquals("Chabon, Michael.", record0.get("/100[0]/a[0]").getValueAsString());

      //Field 245
      // 245 10$aThe amazing adventures of Kavalier and Clay :$ba novel /$cMichael Chabon.
      assertEquals(1, record0.get("/245").getValueAsList().size());
      assertEquals('1', record0.get("/245[0]/indicator1").getValueAsChar());
      assertEquals('4', record0.get("/245[0]/indicator2").getValueAsChar());
      assertEquals(1, record0.get("/245[0]/a").getValueAsList().size());
      assertEquals("The amazing adventures of Kavalier and Clay :", record0.get("/245[0]/a[0]").getValueAsString());
      assertEquals(1, record0.get("/245[0]/b").getValueAsList().size());
      assertEquals("a novel /", record0.get("/245[0]/b[0]").getValueAsString());
      assertEquals(1, record0.get("/245[0]/c").getValueAsList().size());
      assertEquals("Michael Chabon.", record0.get("/245[0]/c[0]").getValueAsString());


      Record record1 = output.getRecords().get("output").get(1);
      assertEquals(Field.Type.LIST_MAP, record1.get().getType());

      Field leader = record1.get("/leader");
      assertEquals("00714cam a2200205 a 4500", leader.getValueAsString());
      _001 = record1.get("/001");
      assertEquals("12883376", _001.getValueAsString());
      _005 = record1.get("/005");
      assertEquals("20030616111422.0", _005.getValueAsString());
      _008 = record1.get("/008");
      assertEquals("020805s2002    nyu    j      000 1 eng  ", _008.getValueAsString());
      Field _020 = record1.get("/020");
      assertEquals(Field.Type.LIST, _020.getType());
      List<Field> _020List = _020.getValueAsList();
      assertEquals(2, _020List.size());
      assertSame(record1.get("/020[0]"), _020List.get(0));
      Field _020_0 = _020List.get(0);
      assertEquals(Field.Type.LIST_MAP, _020_0.getType());
      assertEquals(' ', record1.get("/020[0]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/020[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/020[0]/a").getValueAsList().size());
      assertEquals("0786808772", record1.get("/020[0]/a[0]").getValueAsString());
      assertEquals(' ', record1.get("/020[1]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/020[1]/indicator2").getValueAsChar());
      assertEquals("0786816155 (pbk.)", record1.get("/020[1]/a[0]").getValueAsString());

      //Field 040
      // 040   $aDLC$cDLC$dDLC
      assertEquals(1, record1.get("/040").getValueAsList().size());
      assertEquals(' ', record1.get("/040[0]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/040[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/040[0]/a").getValueAsList().size());
      assertEquals("DLC", record1.get("/040[0]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/040[0]/c").getValueAsList().size());
      assertEquals("DLC", record1.get("/040[0]/c[0]").getValueAsString());
      assertEquals(1, record1.get("/040[0]/d").getValueAsList().size());
      assertEquals("DLC", record1.get("/040[0]/d[0]").getValueAsString());

      //Field 100
      //100 1 $aChabon, Michael.
      assertEquals(1, record1.get("/100").getValueAsList().size());
      assertEquals('1', record1.get("/100[0]/indicator1").getValueAsChar());
      assertEquals(' ', record1.get("/100[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/100[0]/a").getValueAsList().size());
      assertEquals("Chabon, Michael.", record1.get("/100[0]/a[0]").getValueAsString());

      //Field 245
      // 245 10$aSummerland /$cMichael Chabon.
      assertEquals(1, record1.get("/245").getValueAsList().size());
      assertEquals('1', record1.get("/245[0]/indicator1").getValueAsChar());
      assertEquals('0', record1.get("/245[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/245[0]/a").getValueAsList().size());
      assertEquals("Summerland /", record1.get("/245[0]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/245[0]/c").getValueAsList().size());
      assertEquals("Michael Chabon.", record1.get("/245[0]/c[0]").getValueAsString());

      //Fields 650
      // 650  1$aFantasy.
      // 650  1$aBaseball$vFiction.
      // 650  1$aMagic$vFiction.
      assertEquals(3, record1.get("/650").getValueAsList().size());
      assertEquals(' ', record1.get("/650[0]/indicator1").getValueAsChar());
      assertEquals('1', record1.get("/650[0]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/650[0]/a").getValueAsList().size());
      assertEquals("Fantasy.", record1.get("/650[0]/a[0]").getValueAsString());
      assertEquals(' ', record1.get("/650[1]/indicator1").getValueAsChar());
      assertEquals('1', record1.get("/650[1]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/650[1]/a").getValueAsList().size());
      assertEquals("Baseball", record1.get("/650[1]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/650[1]/v").getValueAsList().size());
      assertEquals("Fiction.", record1.get("/650[1]/v[0]").getValueAsString());
      assertEquals(' ', record1.get("/650[2]/indicator1").getValueAsChar());
      assertEquals('1', record1.get("/650[2]/indicator2").getValueAsChar());
      assertEquals(1, record1.get("/650[2]/a").getValueAsList().size());
      assertEquals("Magic", record1.get("/650[2]/a[0]").getValueAsString());
      assertEquals(1, record1.get("/650[2]/v").getValueAsList().size());
      assertEquals("Fiction.", record1.get("/650[2]/v[0]").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }
}
