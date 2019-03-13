package com.example.demo;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.classify.SubclassClassifier;
import org.springframework.retry.RetryPolicy;

import java.util.Map;

public class UnrwappingSubclassClassifier extends SubclassClassifier<Throwable, RetryPolicy> {

    private static final long serialVersionUID = 1462093244761698381L;

    public UnrwappingSubclassClassifier(Map<Class<? extends Throwable>, RetryPolicy> typeMap, RetryPolicy defaultValue) {
        super(typeMap, defaultValue);
    }

    @Override
    public RetryPolicy classify(Throwable classifiable) {
        return super.classify(ExceptionUtils.getRootCause(classifiable));
    }
}
