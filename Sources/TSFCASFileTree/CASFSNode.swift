// This source file is part of the Swift.org open source project
//
// Copyright (c) 2020 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See http://swift.org/LICENSE.txt for license information
// See http://swift.org/CONTRIBUTORS.txt for the list of Swift project authors

import Foundation

import TSFCAS


/// A CAS object (can be tree or blob)
public struct LLBCASFSNode {
    public enum Error: Swift.Error {
        case notApplicable
    }

    public enum NodeContent {
        case tree(LLBCASFileTree)
        case blob(LLBCASBlob)
        case symlink(to: String, id: LLBDataID)
    }

    public let db: LLBCASDatabase
    public let value: NodeContent

    public init(tree: LLBCASFileTree, db: LLBCASDatabase) {
        self.db = db
        self.value = NodeContent.tree(tree)
    }

    public init(blob: LLBCASBlob, db: LLBCASDatabase) {
        self.db = db
        self.value = NodeContent.blob(blob)
    }

    public init(symlink: String, id: LLBDataID, db: LLBCASDatabase) {
        self.db = db
        self.value = NodeContent.symlink(to: symlink, id: id)
    }

    /// Returns aggregated (for trees) or regular size of the Entry
    public func size() -> Int {
        switch value {
        case .tree(let tree):
            return tree.aggregateSize
        case .blob(let blob):
            return blob.size
        case .symlink:
            return 0
        }
    }

    /// Gives CASFSNode type (meaningful for files)
    public func type() -> LLBFileType {
        switch value {
        case .tree(_):
            return .directory
        case .blob(let blob):
            return blob.type
        case .symlink:
            return .symlink
        }
    }

    /// Optionally chainable tree access
    public var tree: LLBCASFileTree? {
        guard case .tree(let tree) = value else {
            return nil
        }
        return tree
    }

    /// Optionally chainable blob access
    public var blob: LLBCASBlob? {
        guard case .blob(let blob) = value else {
            return nil
        }
        return blob
    }

    /// Get the target of a symlink (if symlink).
    public var symlink: String? {
        guard case let .symlink(target, _) = value else {
            return nil
        }
        return target
    }

    public func asDirectoryEntry(filename: String) -> LLBDirectoryEntryID {
        switch value {
        case let .tree(tree):
            return tree.asDirectoryEntry(filename: filename)
        case let .blob(blob):
            return blob.asDirectoryEntry(filename: filename)
        case let .symlink(_, id):
            return LLBDirectoryEntryID(info: .init(name: filename, type: .symlink, size: 0), id: id)
        }
    }
}
