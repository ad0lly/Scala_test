import java.io.File;

public class Test {
        public static void dirAll(File file, int level) {
            for(int i=0;i<level;i++) {
                System.out.print("-");
            }
            System.out.println(file.getName());
            if(file.isDirectory()) {
                File[] files= file.listFiles();
                for(File f:files) {
                    dirAll(f,level+1);
                }
            }
        }
        public static void main(String[] args) {
            File f=new File("C:\\Users\\Lost_\\src");
            dirAll(f,1);
        }
    }

