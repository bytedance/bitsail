package com.bytedance.bitsail.connector.legacy.larksheet.api.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author yangyun
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
public class AppAccessTokenResponse extends OpenApiBaseResponse {

  /**
   * app_access_token
   */
  private String appAccessToken;

  /**
   * expire time in seconds
   */
  private int expire;

  /**
   * tenant_access_token
   */
  private String tenantAccessToken;

}