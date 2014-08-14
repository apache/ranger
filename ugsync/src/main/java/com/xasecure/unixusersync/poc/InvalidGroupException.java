package com.xasecure.unixusersync.poc;

public class InvalidGroupException extends Exception

{
     private static final long serialVersionUID = 1L;
     private final String line;

     public InvalidGroupException(final String msg, final String ln)
     {
         super(msg);

         line = ln;
     }

     public String getLine()
     {
         return (line);
     }
}