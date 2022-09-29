package com.bytedance.bitsail.client.entry;

import com.bytedance.bitsail.client.api.command.BaseCommandArgsWithUnknownOptions;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EntryTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBuildCommandArgsWithEmptyArgs() {
    String[] args = {};
    Entry.buildCommandArgs(args);
  }

  @Test
  public void testBuildCommandArgs() {
    String[] args = new String[] {"run", "--engine", "flink", "-d", "-sae"};
    BaseCommandArgsWithUnknownOptions commandArgsWithUnknownOptions =
        Entry.buildCommandArgs(args);
    assertEquals(commandArgsWithUnknownOptions.getUnknownOptions().length, 1);
    assertEquals(commandArgsWithUnknownOptions.getBaseCommandArgs().getEngineName(), "flink");
    assertEquals(commandArgsWithUnknownOptions.getBaseCommandArgs().getMainAction(), "run");
  }
}
