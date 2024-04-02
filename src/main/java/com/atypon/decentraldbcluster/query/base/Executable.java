package com.atypon.decentraldbcluster.query.base;

public interface Executable<T extends Query> {
    Object exec(T query) throws Exception;
}
