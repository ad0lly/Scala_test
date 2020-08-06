import java.util.Arrays;

public class Object {

    public static void main(String[] args) {
        Person[] persons = new Person[5];
        persons[0] =new Person("tom",45);
        persons[1] =new Person("jack",12);
        persons[2] =new Person("bill",21);
        persons[3] =new Person();
        Arrays.sort(persons);
        for (Person person:persons) {
            System.out.println(person);
        }
    }
}
class Person implements Comparable<Person>{
    private String name;
    private int age;
    public Person(String name,int age){
        this.name = name;
        this.age = age;
    }
    public Person(){
        this("unknown", 0);
    }
    @Override
    public int compareTo(Person o) {
        return age-o.age;
    }
    @Override
    public String toString() {
        return "Person[name:"+name+",age:"+age+"]";
    }



        }