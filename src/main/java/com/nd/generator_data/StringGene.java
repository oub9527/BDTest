package com.nd.generator_data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;

/**
 * @author: oubin
 * @date: 2018/9/30 08:38
 * @Description:
 */
public class StringGene {

    public static void main(String[] args) throws Exception {

        String filePath = "E:\\countFileSmall.txt";
        wirte2Tile(filePath);
    }

    public static void wirte2Tile(String filePath) throws Exception{
        FileWriter fileWriter = new FileWriter(filePath, true);
        Date date = new Date(System.currentTimeMillis());
        System.out.println(date);
        BufferedWriter bw = new BufferedWriter(fileWriter);
        for (int i = 0; i < 5; i++) {
            for(int j = 0; j < 10 ; j ++) {
                Random random = new Random();
                int size = random.nextInt(10);
                String each = getRandomString(size);
                bw.append(each);
                bw.append(" ");
            }
            bw.append("\r\n");
            bw.flush();
        }
        bw.close();
        fileWriter.close();
        System.out.println();

        date = new Date(System.currentTimeMillis());
        System.out.println(date);
    }

    //length用户要求产生字符串的长度
    public static String getRandomString(int length){
        String str="abcdefg0123456789";
        Random random=new Random();
        StringBuilder sb=new StringBuilder();
        for(int i=0;i<length;i++){
            int number=random.nextInt(17);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

}
