package com.bytedance.bitsail.client.api.command;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class BaseCommandArgsWithUnknownOptions {
  private BaseCommandArgs baseCommandArgs;
  private String[] unknownOptions;
}