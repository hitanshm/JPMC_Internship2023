package org.example;

import jnr.ffi.annotations.In;

public class AccountDetails {
    //make private
    public int accountId;
    public String name;
    public int balance;
    //have different names for argument
    public AccountDetails(int accId, String nm, int bal){
        accountId=accId;
        name=nm;
        balance=bal;
    }

    public int getAccountId() {
        return accountId;
    }

    public String toString() {
        return accountId + " " + name + " " + balance;
    }
    //make get functions and set
}
