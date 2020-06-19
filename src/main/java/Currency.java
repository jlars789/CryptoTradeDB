
public class Currency {
	
	private String code;
	private double[] val;
	
	
	
	public Currency(String code, int length) {
		this.code = code;
		this.val = new double[length];
	}
	
	public void setVal(double val, int i) {
		this.val[i] = val;
	}
	
	public double getAverage() {
		double sum = 0;
		for(int i = 0; i < this.getSize(); i++) {
			sum += this.getAtIndex(i);
		}
		return sum/this.getSize();
	}
	
	public void clearArr() {
		int s = this.getSize();
		this.val = new double[s];
	}
	
	public String getCode() {
		return this.code;
	}
	
	public int getSize() {
		return val.length;
	}
	
	public double getAtIndex(int index) {
		return val[index];
	}

}
