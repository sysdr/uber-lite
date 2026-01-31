package com.uberlite.config;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class CustomErrorController implements ErrorController {

    private static final String ERROR_HTML = """
        <!DOCTYPE html>
        <html>
        <head><meta charset="utf-8"><title>Lesson 11</title></head>
        <body style="font-family: system-ui; margin: 2rem;">
        <h1>Page not found</h1>
        <p>Go to <a href="/dashboard">Dashboard</a> or <a href="/">Home</a>.</p>
        </body>
        </html>
        """;

    @RequestMapping("/error")
    public ResponseEntity<String> handleError(HttpServletRequest request) {
        Object status = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
        HttpStatus httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        if (status != null) {
            int code = Integer.parseInt(status.toString());
            httpStatus = HttpStatus.valueOf(code);
        }
        return ResponseEntity.status(httpStatus)
                .contentType(MediaType.parseMediaType("text/html;charset=UTF-8"))
                .body(ERROR_HTML);
    }
}
