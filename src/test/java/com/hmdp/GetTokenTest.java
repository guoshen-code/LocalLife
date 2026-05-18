package com.hmdp;

import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@SpringBootTest
public class GetTokenTest {

    public static void main(String[] args) {
        String loginUrl = "http://localhost:8081/user/login"; // 登录接口
        int count = 1000; // 生成 1000 个 token

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("tokens.txt"))) {
            for (int i = 0; i < count; i++) {
                // 1. 生成手机号
                String phone = String.format("138%08d", i);

                // 2. 构造 JSON，每次 code 和 password 固定为 "123456"
                String json = String.format(
                        "{\"phone\":\"%s\",\"code\":\"123456\"}",
                        phone
                );

                try {
                    // 3. 调用登录接口
                    HttpURLConnection conn = (HttpURLConnection) new URL(loginUrl).openConnection();
                    conn.setRequestMethod("POST");
                    conn.setDoOutput(true);
                    conn.setRequestProperty("Content-Type", "application/json");

                    try (OutputStream os = conn.getOutputStream()) {
                        os.write(json.getBytes(StandardCharsets.UTF_8));
                    }

                    // 4. 读取响应
                    StringBuilder sb = new StringBuilder();
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            sb.append(line);
                        }
                    }
                    String response = sb.toString();

                    // 5. 提取 token（假设返回格式 {"success":true,"data":"<token>"}）
                    String token = response.replaceAll(".*\"data\"\\s*:\\s*\"([^\"]+)\".*", "$1");
                    // 6. 写入文件
                    writer.write(token);
                    writer.newLine();
                    System.out.println("生成 token: " + token);

                } catch (Exception e) {
                    System.err.println("请求失败，手机号：" + phone + "，错误：" + e.getMessage());
                }
            }

            System.out.println("已生成 1000 个 token，保存在 tokens.txt");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

