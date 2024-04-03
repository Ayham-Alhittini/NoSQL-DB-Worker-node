package com.atypon.decentraldbcluster.services;

import java.util.ArrayList;
import java.util.List;

public class ListCaster {
    public static <T> List<T> castList(List<?> rawList, Class<T> elementType) {
        List<T> castedList = new ArrayList<>(rawList.size());
        for (Object object : rawList) {
            if (elementType.isInstance(object)) {
                castedList.add(elementType.cast(object));
            } else {
                throw new ClassCastException("List contains an item of type " + object.getClass().getName() + " but expected " + elementType.getName());
            }
        }
        return castedList;
    }
}
