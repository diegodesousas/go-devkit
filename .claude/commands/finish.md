---
description: Fecha a branch atual e limpa o ambiente git depois do merge
allowed-tools: Bash(git status:*), Bash(git branch:*), Bash(git checkout:*), Bash(git pull:*), Bash(git fetch:*), Bash(git worktree:*), Bash(git stash list), Bash(git push origin --delete:*), Bash(gh pr:*), Read
---

Encerra o trabalho da branch atual conforme a regra em [CLAUDE.md](CLAUDE.md): PR mergeado e ambiente git limpo, sem branch órfã, worktree solta ou stash pendente.

## 1. Onde estamos

`git status` e `git branch --show-current`.

Se a branch atual for `main`, **pare aqui** — não há o que fechar. Se houver alterações não commitadas, reporte e pare: nada de limpeza com trabalho pendente na mesa.

## 2. Estado do PR

`gh pr view --json number,state,url,mergedAt` na branch atual. Três desfechos:

- **Não existe PR** → diga isso, sugira `gh pr create --base main`, e **pare**. Não abra o PR por conta própria.
- **PR aberto** → reporte número e URL, e **pare**. Não rode `gh pr merge`; o merge é decisão do usuário.
- **PR mergeado** (`state: MERGED`) → siga para a limpeza.

## 3. Limpeza, nesta ordem

```
git checkout main
git pull
git branch -d <branch>
git fetch --prune
```

Depois do `prune`, cheque se a branch remota ainda existe (`git branch -r`). Normalmente não existe — o GitHub apaga no merge. Se sobreviveu, apague: `git push origin --delete <branch>`.

Se o trabalho usou worktree: `git worktree remove <path>` e depois `git worktree prune`.

Por fim, `git stash list`. Se não estiver vazio, **avise e deixe como está** — nunca dropar stash sem perguntar.

## 4. Prova

Feche com `git status` e `git branch -a`, mostrando que sobrou só `main` (local e remota) e que a working tree está limpa.

## Armadilhas

- `git branch -d` recusa a branch quando o merge foi feito por **squash** — o git não reconhece o conteúdo como mergeado. Só use `-D` depois que o passo 2 confirmou `state: MERGED`; aí a perda é impossível.
- `git push origin --delete` numa branch que já não existe no remoto retorna erro. É no-op, não é falha — siga em frente.
- `delete_branch_on_merge` está **ligado** neste repositório: a branch remota some no merge do PR. O `git push origin --delete` só entra em cena para branches anteriores a essa configuração ou que nunca viraram PR — cheque antes com `git branch -r`, não dispare por reflexo.
