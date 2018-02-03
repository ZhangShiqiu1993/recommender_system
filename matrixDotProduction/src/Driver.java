public class Driver {
	public static void main(String[] args) throws Exception {
		MatrixTranspose matrixTranspose = new MatrixTranspose();
		DotProduct dotProduct = new DotProduct();

		String m1 = args[0];
		String m2 = args[1];
		String step1_output = args[2];
		String output = args[3];

		String[] path1 = {m2, step1_output};
		String[] path2 = {m1, step1_output ,output};
		matrixTranspose.main(path1);
		dotProduct.main(path2);
	}
}
