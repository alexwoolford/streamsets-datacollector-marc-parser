package io.woolford.stage.processor.marcparser;

import com.streamsets.pipeline.api.Label;


public enum MarcFormat implements Label {
    XML("XML"),
    ISO_2709("ISO 2709");

  private final String label;

  MarcFormat (String label){
    this.label=label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
