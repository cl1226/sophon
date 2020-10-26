package com.scistor.compute.until;

import javax.tools.*;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class CompileUtils {
    public String processA(){
        return "a";
    }
    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String codes = "public static void main(String[]args){" +
                "System.out.print(\"hello world hahah\"); }" +
                "public static String processA(){" +
                "        return \"a\";" +
                "    }";

        StringBuilder sb = new StringBuilder();
        sb.append("package com.scistor.classes;");
        sb.append("\n public class Eval{\n ");
        sb.append(codes);
        sb.append("\n}");
//        Class<?> clazz = compile("com.scistor.classes.Eval", sb.toString());

        Method processA = getMethodByCode("com.scistor.classes.Eval", sb.toString(), "processA");
        Method main = getMethodByCode("com.scistor.classes.Eval", sb.toString(), "main");

        Class<?> aClass = Class.forName("com.scistor.classes.Eval");
        Object o = aClass.newInstance();
        System.out.println(1);


    }
    /**
     * 装载字符串成为java可执行文件
     *
     * @param className className
     * @param javaCodes javaCodes
     * @return Class
     */
    public static Class<?> compile(String className, String javaCodes) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        StrSrcJavaObject srcObject = new StrSrcJavaObject(className, javaCodes);
        Iterable<? extends JavaFileObject> fileObjects = Arrays.asList(srcObject);
        String flag = "-d";
        String outDir = "";
        try {
            File classPath = new File(Thread.currentThread().getContextClassLoader().getResource("").toURI());
            outDir = classPath.getAbsolutePath() + File.separator;
        } catch (URISyntaxException e1) {
            e1.printStackTrace();
        }
        Iterable<String> options = Arrays.asList(flag, outDir);
        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, options, null, fileObjects);
        boolean result = task.call();
        if (result == true) {
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static Method getMethodByCode(String className, String javaCodes, String methodName){
        StringBuilder sb = new StringBuilder();
        sb.append("package com.scistor.classes;");
        sb.append("\n public class "+className+"{\n ");
        sb.append(javaCodes);
        sb.append("\n}");
        Class<?> clazz = compile("com.scistor.classes."+className, sb.toString());
        try {
            for (Method m:clazz.getMethods()) {
                if(m.getName().equals(methodName))
                    return m;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Object invokeMethodByCode(String className, String javaCodes, String methodName, Object... args){
        StringBuilder sb = new StringBuilder();
        sb.append("package com.scistor.classes;");
        sb.append("\n public class "+className+"{\n ");
        sb.append(javaCodes);
        sb.append("\n}");
        Class<?> clazz = compile("com.scistor.classes."+className, sb.toString());
        try {
            // 生成对象
            Object obj = clazz.newInstance();
            Class<? extends Object> cls = obj.getClass();
            for (Method m:clazz.getMethods()) {
                if(m.getName().equals(methodName)){
                    Object invoke = m.invoke(obj, args);
                    return invoke;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static class StrSrcJavaObject extends SimpleJavaFileObject {
        private String content;

        StrSrcJavaObject(String name, String content) {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.content = content;
        }

        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return content;
        }
    }
}
