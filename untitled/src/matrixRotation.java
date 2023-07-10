public class matrixRotation {

    public static void main(String[] args) {

        int[][] matrix = new int[5][5];
        initMatrix(matrix);
        //printMatrix(matrix);
        if(matrix.length==matrix[0].length)
        {
           // rotateclockwiseninety(matrix);
            rotateclockwiseoneeighty(matrix);
            //rotateclockwisetwoseventy(matrix);
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
        m=transpose(m); //transpose
        int temp = 0;

       // printMatrix(m);
        for (int i=0;i<m.length;i++) //swap column order
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
        //System.out.println("180");
        int temp=0;
        //printMatrix(m);
        for (int i =0 ; i<m.length/2;i++)
        {
            for(int j=0;j<m[0].length;j++)
            {
                //System.out.println("swap "+m[i][j]+" with "+m[m.length-1-i][m[0].length-1-j]);
                temp= m[i][j];
                m[i][j]=m[m.length-1-i][m[0].length-1-j];
                m[m.length-1-i][m[0].length-1-j]=temp;

            }

        }
        if(m.length % 2 == 1)
        {
            for (int x=0;x<m.length/2;x++)
            {
                temp = m[m.length/2][x];
                m[m.length/2][x]=m[m.length/2][m[0].length-x-1];
                m[m.length/2][m[0].length-x-1]=temp;
            }
        }
        printMatrix(m);

    }
    public static void rotateclockwisetwoseventy(int a[][]) //90Anticlock
    {

        a=transpose(a);
        int temp;

            for (int i = 0; i < a[0].length; i++) {
                int l = 0;
                int r = a.length-1;
                while (l<=r) {

                    temp = a[l][i];
                    a[l][i]=a[r][i];
                    a[r][i]=temp;

                  ++l;
                  r--;
                }
            //System.out.println(i);

            }




        printMatrix(a);
    }
}
