---
layout: doc_page
---

# Lookups Introduction

Lookups are a means of modifying otherwise immutable data at query time.
A common example of this is when a machine readable ID is described by
some attributes or human friendly names. For example, an entry in a 
data source might have a country code associated with it. A lookup
might have the mapping between country code and locale friendly name,
or country code and primary language spoken in that country.

Lookups allow storing of an arbitrary ID, and the definition of tables
which allow mapping onto new values at query time without modifying the
underlying data. In general computation terms, this is a distributed
hash join.

# Configuration

