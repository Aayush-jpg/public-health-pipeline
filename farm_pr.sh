#!/bin/bash
for i in {1..10}; do
  BRANCH="tweak-$(date +%s)"
  git checkout -b "$BRANCH"
  echo "// minor update $(date)" >> notes.md
  git add .
  git commit -m "minor update $i"
  git push origin "$BRANCH"
  gh pr create --title "Minor update $i" --body "small tweak" --base main --head "$BRANCH"
  gh pr merge "$BRANCH" --merge --delete-branch
  git checkout main
  git pull
  sleep 2
done
