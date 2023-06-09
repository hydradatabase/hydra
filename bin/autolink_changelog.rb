#!/usr/bin/env ruby

# usage: bin/autolink_changelog.rb < CHANGELOG.rb > LINKED-CHANGELOG.rb

require 'set'

PR_REGEXP = /(?<!\[)(#\d+)/
COMMIT_REGEXP = /(?<!\[)\b([\da-f]{7,40})\b/
LINK_REGEXP = /^\[([^\]]+)\]:\s[^\n]+/

prs = Set.new
commits = Set.new
links = Set.new

while line = STDIN.gets
  if m = line.match(LINK_REGEXP)
    links << m[1]
  else
    line = line.gsub(PR_REGEXP) do |match|
      prs << match
      "[#{match}][]"
    end
    line = line.gsub(COMMIT_REGEXP) do |match|
      commits << match
      "[#{match}][]"
    end
  end
  puts line
end

prs.each do |pr|
  next if links.include?(pr)
  puts "[#{pr}]: https://github.com/hydradatabase/hydra/pull/#{pr.sub('#', '')}"
end
commits.each do |commit|
  next if links.include?(commit)
  puts "[#{commit}]: https://github.com/hydradatabase/hydra/commit/#{commit}"
end
