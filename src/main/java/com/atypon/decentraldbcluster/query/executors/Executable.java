package com.atypon.decentraldbcluster.query.executors;

import com.atypon.decentraldbcluster.query.types.Query;

public interface Executable<T extends Query> {
    Object exec(T query) throws Exception;
}
