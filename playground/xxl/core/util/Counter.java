package xxl.core.util;

public class Counter {

	int value;

	public Counter(int value) {
		super();
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}
	
	public int add(int addend) {
		value += addend;
		return value;
	}
	
	public int increment() {
		return add(1);
	}
}
