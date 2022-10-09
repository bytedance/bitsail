package com.bytedance.bitsail.client.entry;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;

import org.junit.Assert;
import org.junit.Test;

public class EntryTest {

  @Test
  public void testBuildCommandArgs() {
    String[] args = new String[] {"run", "--engine", "flink", "-d", "-sae"};
    BaseCommandArgs baseCommandArgs = Entry.loadCommandArguments(args);
    Assert.assertEquals(baseCommandArgs.getUnknownOptions().length, 1);
    Assert.assertEquals(baseCommandArgs.getEngineName(), "flink");
    Assert.assertEquals(baseCommandArgs.getMainAction(), "run");
  }
}
