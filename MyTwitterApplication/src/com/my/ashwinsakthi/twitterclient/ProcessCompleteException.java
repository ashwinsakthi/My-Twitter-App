package com.my.ashwinsakthi.twitterclient;

public class ProcessCompleteException extends Exception {

	//Parameterless Constructor
    public ProcessCompleteException() {}

    //Constructor that accepts a message
    public ProcessCompleteException(String message)
    {
       super(message);
    }
}
