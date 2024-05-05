package com.nimesa.careers;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

@SpringBootApplication
public class CareersApplication {

	public static void main(String[] args) {
		SpringApplication.run(CareersApplication.class, args);
	}

}
