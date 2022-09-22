package com.woowacourse.momo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class ShootController {

    @Autowired
    DataGenerator dataGenerator;

    @GetMapping("/shoot")
    public ResponseEntity<Void> shoot() throws InterruptedException {
        dataGenerator.dbInit();
        return ResponseEntity.ok().build();
    }
}
