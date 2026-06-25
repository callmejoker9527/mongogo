#!/usr/bin/env bash
# publish.sh — 提交代码并推送新版本到 GitHub
# 用法:
#   bash publish.sh              # 自动生成 patch 版本 (v1.0.0 -> v1.0.1)
#   bash publish.sh v1.2.0       # 指定版本号
#   bash publish.sh v1.2.0 "feat: add UpdateOne and DeleteMany"  # 指定版本+提交信息

set -euo pipefail

# ────────────────────────────────────────────────
# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ────────────────────────────────────────────────
# 切换到脚本所在目录（项目根目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ────────────────────────────────────────────────
# 检查必要工具
command -v git  >/dev/null 2>&1 || error "未找到 git，请先安装"
command -v go   >/dev/null 2>&1 || error "未找到 go，请先安装 Go 工具链"

# ────────────────────────────────────────────────
# 编译检查
info "检查编译..."
go build ./mongogo/... || error "编译失败，请修复错误后重试"
info "编译通过"

# ────────────────────────────────────────────────
# 计算新版本号
LATEST_TAG=$(git tag --sort=-version:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1 || true)

if [[ -n "$1" ]] 2>/dev/null && [[ "$1" == v* ]]; then
    NEW_TAG="$1"
    COMMIT_MSG="${2:-"release: ${NEW_TAG}"}"
else
    COMMIT_MSG="${1:-""}"
    if [[ -z "$LATEST_TAG" ]]; then
        NEW_TAG="v1.0.0"
    else
        # 自动递增 patch 版本
        IFS='.' read -r MAJOR MINOR PATCH <<< "${LATEST_TAG#v}"
        NEW_TAG="v${MAJOR}.${MINOR}.$((PATCH + 1))"
    fi
    if [[ -z "$COMMIT_MSG" ]]; then
        COMMIT_MSG="release: ${NEW_TAG}"
    fi
fi

info "当前最新 tag: ${LATEST_TAG:-（无）}"
info "新版本 tag:   ${NEW_TAG}"
info "提交信息:     ${COMMIT_MSG}"

# ────────────────────────────────────────────────
# 检查是否有未提交的变更
if [[ -n "$(git status --porcelain)" ]]; then
    info "检测到未提交的变更，正在提交..."
    git add -A
    git commit -m "$COMMIT_MSG"
    info "提交完成"
else
    info "没有新的变更需要提交"
fi

# ────────────────────────────────────────────────
# 推送代码
info "推送代码到 origin/main..."
git push origin main || error "推送失败，请检查远程仓库配置或网络"

# ────────────────────────────────────────────────
# 打 tag 并推送
if git tag | grep -q "^${NEW_TAG}$"; then
    warn "tag ${NEW_TAG} 已存在，跳过打 tag"
else
    git tag "$NEW_TAG"
    info "已创建 tag: ${NEW_TAG}"
fi

info "推送 tag 到 GitHub..."
git push origin "$NEW_TAG" || error "推送 tag 失败"

# ────────────────────────────────────────────────
echo ""
info "🎉 发布成功！"
echo ""
echo -e "  ${GREEN}版本:${NC}     ${NEW_TAG}"
echo -e "  ${GREEN}包地址:${NC}   https://pkg.go.dev/github.com/callmejoker9527/mongogo@${NEW_TAG}"
echo -e "  ${GREEN}使用方式:${NC} go get github.com/callmejoker9527/mongogo@${NEW_TAG}"
echo ""

