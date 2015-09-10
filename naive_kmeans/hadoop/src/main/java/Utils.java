import java.util.Vector;

public class Utils{
    public static double measureDistance(Vector<Double> center, Vector<Double> v) {

        double dist = 0.0d;

        for(double val1: center){
            for (double val2: v){
                dist += (val1-val2) * (val1-val2);
            }
        }

        double sqrtdist = Math.sqrt(dist);

        return sqrtdist;
    }

    public static Vector<Double> getOrigin(int dimensions) {
      Vector<Double> origin = new Vector<Double>();

      for(int i=0; i< dimensions; i++){
        origin.add(0.0d);
      }

      return origin;

    }

    public static Vector<Double> getRandomNormalizedVector(int dimensions) {

      Vector<Double> origin = new Vector<Double>();

      for(int i=0; i< dimensions; i++){
        origin.add(Math.random());
      }

      return origin;

    }

    public static Vector<Double> arrayToVector(double[] arr){
        Vector<Double> out = new Vector<Double>();

        for(int i=0; i< arr.length; i++){
            out.add(arr[i]);
        }

        return out;

    }

    public static double[] vectorToArray(Vector<Double> vec){

        double[] out = new double[vec.size()];

        for(int i = 0;i < vec.size();i++){
            out[i] = vec.get(i);
        }

        return out;
    }

    public static Vector<Double> divide(Vector<Double> a, double b){
        if (b == 0d) {
            throw new java.lang.ArithmeticException("/ by zero");
        }
        Vector<Double> v = new Vector<Double>(a.size());
        
        for (int i = 0; i < a.size(); i++) {

            double newPoint = a.get(i) / b;

            v.add(newPoint);
        }
        
        return v;
    }

    public static Vector<Double> add(Vector<Double> a, Vector<Double>b){
        assert a.size() == b.size();
    
        Vector<Double> newv = new Vector<Double>(a.size());

        for (int i = 0; i < b.size(); i++) {
            newv.add(a.get(i) + b.get(i));
        }
    
        return newv;
    }

    public static Vector<Double> deepCopy(Vector<Double> from){
        final double[] src = vectorToArray(from);
        final double[] dest = new double[from.size()];

        System.arraycopy(src, 0, dest, 0, from.size());

        Vector<Double> trgt = arrayToVector(dest);

        return trgt;
    }

    public static Vector<Double> abs(Vector<Double> a){

        Vector<Double> v = new Vector<Double>(a.size());

        for (int i = 0; i < a.size(); i++) {
            v.add(Math.abs(a.get(i)));
        }

        return v;   
    }

    public static boolean converged(Vector<Double> cntr, Vector<Double> vec, double epsilon){

        // if(cntr.size()==0)
        //     return false;

        Vector<Double> v1  = subtract(cntr,vec);
        Vector<Double> v2  = abs(v1);
        double v3  = sum(v2);
        double v4  = Math.sqrt(v3);

        if(v4 > epsilon){
            return true;
        }else{
            return false;
        }

    }

    public static Vector<Double> sqrt(Vector<Double> a){
        Vector<Double> v = new Vector<Double>(a.size());
        for (int i = 0; i < a.size(); i++) {
            v.add(Math.sqrt(a.get(i)));
        }
        return v;   
    }

    public static Vector<Double> getNewCenter(Vector<Double> currentCenter, Vector<Double> newPoint){

        Vector<Double> newCenter = new Vector<Double>();

        assert currentCenter.size() == newPoint.size();

        int size = currentCenter.size();

        for(int i=0; i< size; i++){
            double elem = (double) ( (currentCenter.get(i)+newPoint.get(i)) / 2.0);
            newCenter.add(elem);
        }        

        return newCenter;

    }

    public static Vector<Double> subtract(Vector<Double> a,Vector<Double> o){
        
        // System.out.println(a.toString()+"  ------  "+o.toString());

        Vector<Double> result = new Vector<Double>();

        for(int i = 0; i< o.size(); i++){
            result.add(a.get(i) - o.get(i));
        }

        return result;

    }

    public static double sum(Vector<Double> a){
            
        double sum = 0.0d;    

        for(double elemA: a){
            sum += elemA;
        }

        return sum;
    }


}