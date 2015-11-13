package posmining.target;

public class TargetItem {

	private static final String[] TARGET_CATEGORYS = {"滋養強壮ドリンク（一般品）","滋養強壮ドリンク（医薬部外品）","栄養補給ドリンク","ビネガードリンク","栄養補給錠剤・錠菓","栄養補給菓子","その他栄養補給食品"};

	public boolean isTargetItem(String category){
		for(String targetCategory : TARGET_CATEGORYS){
			if(targetCategory.equals(category)){
				return true;
			}
		}
		return false;
	}

}
