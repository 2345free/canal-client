package com.daysofree.canal.model;

import lombok.Data;

import javax.persistence.Table;
import java.util.Date;

@Data
@Table(name = "t_user")
public class User {

    private Integer id;
    private String nickName;
    private String name;
    private Integer age;
    private Date birthday;

}
