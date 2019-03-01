package io.woolford.stage.processor.marcparser;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class MarcFormatChooserValues extends BaseEnumChooserValues<MarcFormat> {


  public MarcFormatChooserValues() {
    super(
        MarcFormat.ISO_2709,
        MarcFormat.XML
    );
  }
}
