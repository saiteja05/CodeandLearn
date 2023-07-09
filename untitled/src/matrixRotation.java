public class matrixRotation {

    public static void main(String[] args) {

        int[][] matrix = new int[4][4];
        initMatrix(matrix);
        printMatrix(matrix);
        if(matrix.length==matrix[0].length)
        {
           // rotateclockwiseninety(matrix);
            rotateclockwiseoneeighty(matrix);
        }

    }
    public static void initMatrix(int m[][] )
    {
        int counter=1;
        for (int i =0 ; i<m.length;i++)
        {
            for (int j=0;j<m[i].length;j++)
            {
                m[i][j]= counter;
                counter++;

            }
        }
    }

    //transpose

    public static int[][] transpose (int m[][])
    {
        int temp=0;
       //printMatrix(m);
        for (int i =0 ; i<m.length;i++)
        {
            for (int j=i;j<m[i].length;j++)
            {
                 temp=m[i][j];
                m[i][j]=m[j][i];
                m[j][i]=temp;

            }
        }
        //printMatrix(m);

   return m;

    }

//    public static void swap(int a,int b)
//    {
//        int temp = a;
//        a=b;
//        b=temp;
//    }

    public static void printMatrix(int m[][])
    {
        for (int i =0 ; i<m.length;i++)
        {
            for (int j=0;j<m[i].length;j++)
            {
                System.out.println(m[i][j]);

            }
        }
    }
    public static void rotateclockwiseninety(int m[][]) //270Anticlock
    {
        m=transpose(m);
        int temp = 0;

       // printMatrix(m);
        for (int i=0;i<m.length;i++)
        { int r = m[i].length-1;
            int l = 0;
            while (l<=r)
            { //System.out.println(i+","+l+"-"+i+","+r);
               temp= m[i][l];
               m[i][l]=m[i][r];
               m[i][r]=temp;
                l++;
                r--;


            }



        }
        printMatrix(m);




    }
    public static  void rotateclockwiseoneeighty(int m[][])
    {
        System.out.println("180");
        int temp=0;
        //printMatrix(m);
        for (int i =0 ; i<m.length-1;i++)
        {
            for (int j=i;j<m[i].length;j++)
            {   System.out.println(i+"-"+j+","+(m.length-1-i)+"-"+(m[i].length-1-j));
                temp=m[i][j];
                m[i][j]=m[m.length-i-1][m[i].length-1-j];
                m[m.length-1-i][m[i].length-1-j]=temp;

            }
        }
        printMatrix(m);

    }
    public static void rotateclockwisetwoseventy(int m[][]) //90Anticlock
    {

    }
}
