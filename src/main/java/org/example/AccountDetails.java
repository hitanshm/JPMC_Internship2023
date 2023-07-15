package org.example;

public class AccountDetails {
    private int accountID;
    private String name;
    private int balance;

    public AccountDetails(int accountIdentifier, String personName, int bankBalance){
        this.accountID = accountIdentifier;
        this.name = personName;
        this.balance = bankBalance;
    }
}