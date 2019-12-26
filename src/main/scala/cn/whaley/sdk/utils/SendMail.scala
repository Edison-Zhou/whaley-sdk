package cn.whaley.sdk.utils

import java.io.{PrintWriter, StringWriter}

import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.json.{JSONArray, JSONObject}

/**
  * Created by Administrator on 2016/7/19.
  */
object SendMail {
   /**
    * 将异常信息转换为String
    * @param e 异常信息
    * @return 异常的string
    */
   def getErrorInfoFromException(e:Throwable)={
     val sw = new StringWriter()
     val pw = new PrintWriter(sw)
     e.printStackTrace(pw)
     sw.toString
   }

   /**
    * 发送post请求
    * @param e 异常
    * @param subject 文件标题
    * @param emailsName 邮件地址
    * @return
    */
   def post(e:Throwable,subject:String,emailsName:Array[String])={
     val url = "http://mailserver.whaleybigdata.com/"
     try{
       val body = getErrorInfoFromException(e)
       val httpClient = HttpClients.createDefault()
       val httpPost = new HttpPost(url)
       //封装收件人 主题 内容
       val parameters = new JSONObject()
       val emails = new JSONArray()
       for(email<- emailsName)emails.put(email)
       parameters.put("to",emails)
       parameters.put("body",body)
       parameters.put("subject",subject)
       //设置参数
       val se = new StringEntity(parameters.toString,"UTF-8")
       se.setContentType("application/json")
       se.setContentEncoding("UTF-8")
       httpPost.setEntity(se)
       val res = httpClient.execute(httpPost)
       //如果没发送成功 尝试再次发送
       if(res.getStatusLine.getStatusCode != HttpStatus.SC_OK){
         var flag = 0
         while (flag < 3){
           val response = httpClient.execute(httpPost)
           if(response.getStatusLine.getStatusCode == HttpStatus.SC_OK)
             flag = 3
           else
             flag = flag + 1
         }
       }
     }catch {
       case e:Exception =>{
         e.printStackTrace()
       }
     }
   }

 }
