import java.util.List;
import java.util.ArrayList;
public class PascalsTriangle {

    public List<ArrayList> generate(int numRows) {

        List<ArrayList> output = new ArrayList<>();
        output.add(new ArrayList(1));
        output.get(0).add(1);

        int i;
        for(i=1;i<numRows;i++)
        {
            ArrayList<Integer> row = new ArrayList(i+1);
            ArrayList<Integer> prow =  output.get(i-1);

            row.add(1);

            for(int j = 1;j< prow.size();j++)
            {
                row.add( prow.get(j-1) + prow.get(j));

            }

            row.add(1);
            output.add(row);

        }





    return output;
    }
}
