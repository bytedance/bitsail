package com.bytedance.bitsail.client.api.command;

import com.beust.jcommander.ParameterException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Created 2022/8/11
 */
public class BaseCommandArgsParserTest {
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testBaseCommandArgs() {
    String[] args = new String[] {"--engine", "flink", "-d", "-sae"};

    BaseCommandArgs baseCommandArgs = new BaseCommandArgs();
    String[] strings = CommandArgsParser.parseArguments(args, baseCommandArgs);

    assertEquals(1, strings.length);
    assertEquals(strings[0], "-sae");

    args = new String[] {"--engine", "flink", "--conf", "test", "--props", "key=value"};
    strings = CommandArgsParser.parseArguments(args, baseCommandArgs);
    assertEquals(baseCommandArgs.getProperties().size(), 1);
    assertEquals(0, strings.length);
  }

  @Test(expected = ParameterException.class)
  public void testDynamicParameter() {
    String[] args = new String[] {"--engine", "flink", "--props", "key"};
    BaseCommandArgs baseCommandArgs = new BaseCommandArgs();
    CommandArgsParser.parseArguments(args, baseCommandArgs);
    exceptionRule.expect(ParameterException.class);
    exceptionRule.expectMessage("Dynamic parameter expected a value of the form a=b but got");
  }
}