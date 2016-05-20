# Testing on replica

`gh-ost`'s design allows for trusted and reliable tests of the migration without compromising production data integrity.

Test on replica if you:
- Are unsure of `gh-ost`, have not gained confidence into its workings
- Just want to experiment with a real migration without affecting production (maybe measure migration time?)
- Wish to observe data change impact
