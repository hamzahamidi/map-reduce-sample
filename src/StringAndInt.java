
/**
 * @author LERARI
 *
 */
public class StringAndInt implements Comparable<StringAndInt> {

	String tag ;
	int nbrOccurrences;
	
	
	public StringAndInt(String tag, int nbrOccurrences) {
		super();
		this.tag = tag;
		this.nbrOccurrences = nbrOccurrences;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getNbrOccurance() {
		return nbrOccurrences;
	}

	public void setNbrOccurance(int nbrOccurance) {
		this.nbrOccurrences = nbrOccurance;
	}



	@Override
	public int compareTo(StringAndInt toCompare) {		
		return  toCompare.nbrOccurrences - this.nbrOccurrences;
	}

}