package com.xasecure.unixusersync.poc;

public class InvalidUserException extends Exception

{
     private static final long serialVersionUID = 1L;
     private final String line;

     public InvalidUserException(final String msg, final String ln)
     {
         super(msg);

         line = ln;
     }

     public String getLine()
     {
         return (line);
     }
}