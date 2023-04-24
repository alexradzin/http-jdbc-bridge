package com.nosqldriver.jdbc.http.permissions;

import com.nosqldriver.util.function.ThrowingFunction;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Predicate;

public class QueryInvocationHandler implements InvocationHandler {
    private final Predicate<Method> wrapCriteria;
    private final ThrowingFunction<String, String, SQLException> queryValidator;
    private final Statement statement;

    public QueryInvocationHandler(Predicate<Method> wrapCriteria,
                                  ThrowingFunction<String, String, SQLException> queryValidator,
                                  Statement statement) {
        this.wrapCriteria = wrapCriteria;
        this.queryValidator = queryValidator;
        this.statement = statement;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        boolean shouldValidate = wrapCriteria.test(method) &&
                method.getParameterCount() > 0 &&
                String.class.equals(method.getParameterTypes()[0]);
        if (shouldValidate) {
            args[0] = queryValidator.apply((String)args[0]);
        }
        return method.invoke(statement, args);
    }
}
