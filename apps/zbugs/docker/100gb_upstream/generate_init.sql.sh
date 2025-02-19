#!/bin/bash

# Create/overwrite the init.sql file with the base COPY statements
cat > init.sql << 'EOF'
COPY "user"
FROM
    '/data/users.csv' WITH CSV HEADER;

COPY "label"
FROM
    '/data/labels.csv' WITH CSV HEADER;

COPY "issue"
FROM
    '/data/issues.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments.csv' WITH CSV HEADER;
EOF

# Append the numbered COPY statements
for i in $(seq 2 400); do
    echo "COPY \"issue\" FROM '/data/issues_${i}.csv' WITH CSV HEADER;" >> init.sql
    echo "COPY \"issueLabel\" FROM '/data/issue_labels_${i}.csv' WITH CSV HEADER;" >> init.sql
    echo "COPY \"comment\" FROM '/data/comments_${i}.csv' WITH CSV HEADER;" >> init.sql
done 