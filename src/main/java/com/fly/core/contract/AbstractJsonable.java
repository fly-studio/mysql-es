package com.fly.core.contract;

import java.io.File;

public interface AbstractJsonable {

   String toJson();
   void toJson(File file) throws Exception;

}
