## Archived Resources

This repository previously bundled a large `archive/` directory containing
prototype SQL scripts, experimental notebooks, and legacy ML experiments.
Those materials are no longer part of the default codebase to keep the
production pipeline lightweight and easier to maintain.

If you still need any of the historical artifacts, you can retrieve them from
Git history. The final commit that contained the full archive before this
cleanup was `c302f69bbcc08b6a74eb2862042d8dc6431d7a98`.

```bash
# Check out the snapshot with archived assets
git checkout c302f69bbcc08b6a74eb2862042d8dc6431d7a98 -- archive/

# After copying what you need, reset your working tree:
git restore archive/
```

You can also create a dedicated branch to hold the legacy content:

```bash
git branch archive-snapshot c302f69bbcc08b6a74eb2862042d8dc6431d7a98
```

Feel free to move any assets you still rely on into a separate repository or a
cloud bucket so that the main pipeline remains focused on the current
deployment workflow.

