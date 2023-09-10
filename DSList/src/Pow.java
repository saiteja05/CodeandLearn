public class Pow {


    public static void main(String[] args) {

        System.out.println( myPow(2.0,-2147483648));
        System.out.println(myPow2(2.0,-2147483648));

    }

    public static double myPow2(double x, int n) {
        double ans = 1.0;
        long nn = n;
        if (nn < 0) nn = -1 * nn;
        while (nn > 0) {
            if (nn % 2 == 1) {
                ans = ans * x;
                nn = nn - 1;
            } else {
                x = x * x;
                nn = nn / 2;
            }
        }
        if (n < 0) ans = (double)(1.0) / (double)(ans);
        return ans;
    }

    public static double myPow(double x, int n) {
        long nn = n;
        double val=1;
        boolean isneg = false;
        if(nn<0)
        {
            //System.out.println("n is negative");
            nn=nn*-1;
            isneg=true;
        }
        while(nn>0)
        {
            if(nn%2==1)
            {
                val*=x;
                nn--;
            }
            else
            {
                x=x*x;
                nn=nn/2;
            }


        }

        if(isneg) val=(1/val);

        return val;




    }
}
