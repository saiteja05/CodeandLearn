
// strivers list problem 1

/*


Time Complexity: O(M×N)O(M \times N)O(M×N) where M and N are the number of rows and columns respectively.

Space Complexity: O(M+N)O(M + N)O(M+N).


 */

class MatrixZeros {
    public void setZeroes(int[][] m) {
        int i,j;
        int[] rowswithzeros = new int[m.length];
        int[] colswithzeros = new int[m[0].length];
        java.util.Arrays.fill(rowswithzeros,0);
        java.util.Arrays.fill(colswithzeros,0);
        for (i=0; i<m.length;i++){
            for(j=0;j<m[0].length;j++)
            {
                if(m[i][j]==0)
                {
                    rowswithzeros[i]=1;
                    colswithzeros[j]=1;
                }
            }
        }

        // setting the entire row to zero

        for (i=0; i<m.length;i++){
            for(j=0;j<m[0].length;j++)
            {
                if(rowswithzeros[i] ==1 || colswithzeros[j]==1)
                {
                    m[i][j]=0;
                }

            }
        }


    }


    // we will attempt a more space efficient problem
    // time complexiity 0(mn) space complexity low.
    public void setZeros2(int m[][])
    {
        int i,j;
        boolean isCol = m[0][0]==0?true:false;
        // int[] rowswithzeros = new int[m.length];
        // int[] colswithzeros = new int[m[0].length];
        // java.util.Arrays.fill(rowswithzeros,0);
        // java.util.Arrays.fill(colswithzeros,0);
        for (i=0; i<m.length;i++){
            for(j=0;j<m[0].length;j++)
            {
                if(m[i][j]==0)
                {
                    m[i][0]=0;
                    m[0][j]=0;

                }
            }
        }

        // setting the entire row to zero

        for (i=1; i<m.length;i++){
            for(j=1;j<m[0].length;j++)
            {
                if( m[i][0]==0 || m[0][j]==0)
                {
                    m[i][j]=0;
                }

            }
        }

        if(m[0][0]==0)
        {
            for(j=0;j<m[0].length;j++)
            {
                m[0][j]=0;
            }

        }

        if(isCol)
        {
            for(i=0;i<m.length;i++)
            {
                m[i][0]=0;
            }
        }
    }

    }
