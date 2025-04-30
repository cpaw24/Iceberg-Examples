cat = {
	"catalog": {
		"default": {
			"type": "sql",
			"uri": "postgresql+psycopg2://iceb_cat_usr:ZAQ1XSW2@10.0.0.30:5439/ib_cat",
			"warehouse": "/Volumes/ExtShield/warehouse",

			"write.metadata.previous-versions.max": 10,
			"write.data.path": "/Volumes/ExtShield/warehouse",
			"commit.manifest-merge.enabled": "True",
			"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
		},
	},
}
