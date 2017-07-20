package com.example.demo.domain;

public class HbaseUser {

    private String name;
    private String email;
    private String password;

    public HbaseUser(String name, String email, String password) {
        super();
        this.name = name;
        this.email = email;
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "HbaseUser [name=" + name + ", email=" + email + ", password=" + password + "]";
    }


}
