package nc.isi.fragaria_adapter_couchdb;

import org.ektorp.changes.ChangesFeed;

public class ChangeFeedHolder {
	private final ChangesFeed feed;
	private final String dsKey;
	private Integer sequence;

	public ChangeFeedHolder(ChangesFeed feed, String dsKey, Integer sequence) {
		this.feed = feed;
		this.dsKey = dsKey;
		this.sequence = sequence;
	}

	public ChangesFeed getFeed() {
		return feed;
	}

	public String getDsKey() {
		return dsKey;
	}

	public Integer getSequence() {
		return sequence;
	}

	public void setSequence(Integer sequence) {
		this.sequence = sequence;
	}

}
